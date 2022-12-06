package main

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/params"
	"github.com/protolambda/ztyp/view"
)

func createBlobTransaction(t *TestEnv, nonce uint64, amount *big.Int, sourceAddr common.Address, targetAddr common.Address) *types.Transaction {
	gasLimit := 210000

	var blobs types.Blobs
	blobs = append(blobs, types.Blob{})
	kzgCommitments, versionedHashes, aggregatedProof, err := blobs.ComputeCommitmentsAndAggregatedProof()
	if err != nil {
		t.Fatalf("unable to compute kzg commitments: %v", err)
	}
	txData := types.SignedBlobTx{
		Message: types.BlobTxMessage{
			ChainID:             view.MustUint256(chainID.String()),
			Nonce:               view.Uint64View(nonce),
			Gas:                 view.Uint64View(gasLimit),
			GasFeeCap:           view.MustUint256(gasPrice.String()),
			GasTipCap:           view.MustUint256(gasTipCap.String()),
			MaxFeePerDataGas:    view.MustUint256("3000000000"), // needs to be at least the min fee
			Value:               view.MustUint256(amount.String()),
			To:                  types.AddressOptionalSSZ{Address: (*types.AddressSSZ)(&targetAddr)},
			BlobVersionedHashes: versionedHashes,
		},
	}
	wrapData := types.BlobTxWrapData{
		BlobKzgs:           kzgCommitments,
		Blobs:              blobs,
		KzgAggregatedProof: aggregatedProof,
	}
	rawTx := types.NewTx(&txData, types.WithTxWrapData(&wrapData))
	tx, err := t.Vault.signTransaction(sourceAddr, rawTx)
	if err != nil {
		t.Fatalf("Unable to sign value tx: %v", err)
	}
	return tx
}

// blobTransactionTest creates a blob transaction. Then asserts that it is
// included in a block
func blobTransactionTest(t *TestEnv) {
	var (
		sourceAddr  = t.Vault.createAccount(t, big.NewInt(params.Ether))
		sourceNonce = uint64(0)
		targetAddr  = t.Vault.createAccount(t, nil)
	)

	// Get current balance
	sourceAddressBalanceBefore, err := t.Eth.BalanceAt(t.Ctx(), sourceAddr, nil)
	if err != nil {
		t.Fatalf("Unable to retrieve balance: %v", err)
	}

	expected := big.NewInt(params.Ether)
	if sourceAddressBalanceBefore.Cmp(expected) != 0 {
		t.Errorf("Expected balance %d, got %d", expected, sourceAddressBalanceBefore)
	}

	nonceBefore, err := t.Eth.NonceAt(t.Ctx(), sourceAddr, nil)
	if err != nil {
		t.Fatalf("Unable to determine nonce: %v", err)
	}
	if nonceBefore != sourceNonce {
		t.Fatalf("Invalid nonce, want %d, got %d", sourceNonce, nonceBefore)
	}

	amount := big.NewInt(1234)
	valueTx := createBlobTransaction(t, sourceNonce, amount, sourceAddr, targetAddr)
	sourceNonce++

	t.Logf("blobTransactionTest: BalanceAt: send %d wei from 0x%x to 0x%x in 0x%x", valueTx.Value(), sourceAddr, targetAddr, valueTx.Hash())
	if err := t.Eth.SendTransaction(t.Ctx(), valueTx); err != nil {
		t.Fatalf("Unable to send transaction: %v", err)
	}
	t.Logf("blobTransactionTest: Sent Transaction for %v", valueTx.Hash())

	var receipt *types.Receipt
	for {
		receipt, err = t.Eth.TransactionReceipt(t.Ctx(), valueTx.Hash())
		if receipt != nil {
			break
		}
		if err != ethereum.NotFound {
			t.Fatalf("Could not fetch receipt for 0x%x: %v", valueTx.Hash(), err)
		}
		time.Sleep(time.Second)
	}
	t.Logf("blobTransactionTest: receipt for %v", valueTx.Hash())

	// ensure balances have been updated
	accountBalanceAfter, err := t.Eth.BalanceAt(t.Ctx(), sourceAddr, nil)
	if err != nil {
		t.Fatalf("Unable to retrieve balance: %v", err)
	}
	balanceTargetAccountAfter, err := t.Eth.BalanceAt(t.Ctx(), targetAddr, nil)
	if err != nil {
		t.Fatalf("Unable to retrieve balance: %v", err)
	}

	// expected balance is previous balance - tx amount - tx fee (gasUsed * gasPrice)
	exp := new(big.Int).Set(sourceAddressBalanceBefore)
	exp.Sub(exp, amount)
	exp.Sub(exp, new(big.Int).Mul(big.NewInt(int64(receipt.GasUsed)), valueTx.GasPrice()))
	exp.Sub(exp, big.NewInt(int64(len(valueTx.DataHashes())*params.DataGasPerBlob)))

	if exp.Cmp(accountBalanceAfter) != 0 {
		t.Errorf("Expected sender account to have a balance of %d, got %d", exp, accountBalanceAfter)
	}
	if balanceTargetAccountAfter.Cmp(amount) != 0 {
		t.Errorf("Expected new account to have a balance of %d, got %d", valueTx.Value(), balanceTargetAccountAfter)
	}

	// ensure nonce is incremented by 1
	nonceAfter, err := t.Eth.NonceAt(t.Ctx(), sourceAddr, nil)
	if err != nil {
		t.Fatalf("Unable to determine nonce: %v", err)
	}
	expectedNonce := nonceBefore + 1
	if expectedNonce != nonceAfter {
		t.Fatalf("Invalid nonce, want %d, got %d", expectedNonce, nonceAfter)
	}

	block, err := t.Eth.BlockByHash(t.Ctx(), receipt.BlockHash)
	if err != nil {
		t.Errorf("unable to retrieve block: %v", err)
	}
	if block.ExcessDataGas() == nil {
		t.Fatalf("block does not contain excess data gas field")
	}
	var blockTx *types.Transaction
	for _, tx := range block.Transactions() {
		if tx.Hash().Hex() == valueTx.Hash().Hex() {
			blockTx = tx
		}
	}
	if blockTx == nil {
		t.Fatalf("missing blob transaction in block response")
	}
	expectedNumHashes := len(valueTx.DataHashes())
	numHashes := len(blockTx.DataHashes())
	if expectedNumHashes != numHashes {
		t.Fatalf("tx in block contains an invalid number of data hashes. (%d != %d)", expectedNumHashes, numHashes)
	}
}

// feeMarketTest creates multiple transactions to induce excess data gas
func feeMarketTest(t *TestEnv) {
	var (
		sourceAddr  = t.Vault.createAccount(t, big.NewInt(params.Ether))
		sourceNonce = uint64(0)
		targetAddr  = t.Vault.createAccount(t, nil)
		amount      = big.NewInt(0)
		numTxs      = 20
	)

	txs := make([]*types.Transaction, numTxs)
	for i := range txs {
		txs[i] = createBlobTransaction(t, sourceNonce, amount, sourceAddr, targetAddr)
		sourceNonce++
	}

	receipts := make(chan *types.Receipt, len(txs))
	var wg sync.WaitGroup
	wg.Add(len(txs))

	// Note: t.Ctx() is not goroutine-safe.
	tCtx := t.Ctx()
	for _, tx := range txs {
		tx := tx
		go func() {
			defer wg.Done()
			err := t.Eth.SendTransaction(tCtx, tx)
			if err != nil {
				t.Fatalf("Error sending tx (%v): %v", tx.Hash(), err)
			}
			t.Logf("waiting for confirmation of %v", tx.Hash())
			receipt, err := waitForReceipt(tCtx, t.Eth, tx.Hash())
			//receipt, err := waitForTxConfirmations(t, tx.Hash(), 0)
			if err != nil {
				t.Fatalf("failed to wait for tx (%v) confirmation: %v", tx.Hash(), err)
			}
			receipts <- receipt
		}()
	}
	wg.Wait()
	close(receipts)

	blockNumbers := make(map[uint64]bool)
	var blocks []*types.Block
	for receipt := range receipts {
		blocknum := receipt.BlockNumber.Uint64()
		if _, ok := blockNumbers[blocknum]; !ok {
			blockHash := receipt.BlockHash.Hex()
			block, err := t.Eth.BlockByHash(t.Ctx(), common.HexToHash(blockHash))
			if err != nil {
				t.Fatalf("Error getting block: %v", err)
			}
			excessDataGas := block.ExcessDataGas()
			if excessDataGas == nil {
				t.Fatalf("nil excess_blobs in block header. block_hash=%v", blockHash)
			}
			blockNumbers[blocknum] = true
			blocks = append(blocks, block)
		}
	}
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Number().Uint64() < blocks[j].Number().Uint64()
	})

	prevExcessDataGas := new(big.Int)
	parentBlock, err := t.Eth.BlockByHash(t.Ctx(), blocks[0].ParentHash())
	if err != nil {
		t.Fatalf("Error getting block: %v", err)
	}
	if e := parentBlock.ExcessDataGas(); e != nil {
		prevExcessDataGas.Set(e)
	}

	// TODO(EIP-4844): Make this robust against other tests. The computed excess data gas may be incorrect if there are other tests including blob transactions.
	for _, block := range blocks {
		// Assuming each transaction contains a single blob
		expected := misc.CalcExcessDataGas(prevExcessDataGas, len(block.Transactions()))
		if expected.Cmp(block.ExcessDataGas()) != 0 {
			t.Fatalf("unexpected excess_data_gas field in header. expected %v. got %v", expected, block.ExcessDataGas())
		}
		prevExcessDataGas = expected
	}
}

func waitForReceipt(ctx context.Context, client *ethclient.Client, txhash common.Hash) (*types.Receipt, error) {
	var receipt *types.Receipt
	var err error
	for {
		receipt, err = client.TransactionReceipt(ctx, txhash)
		if receipt != nil {
			break
		}
		if err != ethereum.NotFound {
			return nil, fmt.Errorf("%w: TransactionReceipt", err)
		}
		time.Sleep(time.Second)
	}
	return receipt, nil
}
