package data

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/algorand/go-algorand/crypto"
	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/pools"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/data/transactions/verify"
	"github.com/algorand/go-algorand/logging"
	"github.com/algorand/go-algorand/network"
	"github.com/algorand/go-algorand/protocol"
	"github.com/algorand/go-algorand/util/execpool"
	"github.com/algorand/go-algorand/util/metrics"
)

const txBacklogSize = 1000

var transactionMessagesHandled = metrics.MakeCounter(metrics.TransactionMessagesHandled)
var transactionMessagesDroppedFromBacklog = metrics.MakeCounter(metrics.TransactionMessagesDroppedFromBacklog)
var transactionMessagesDroppedFromPool = metrics.MakeCounter(metrics.TransactionMessagesDroppedFromPool)

type txBacklogMsg struct {
	rawmsg            *network.IncomingMessage // the raw message from the network
	unverifiedTxGroup []transactions.SignedTxn // the unverified ( and signed ) transaction group
	verificationErr   error                    // The verification error generated by the verification function, if any.
}

type TxHandler struct {
	txPool                *pools.TransactionPool
	ledger                *Ledger
	genesisID             string
	genesisHash           crypto.Digest
	txVerificationPool    execpool.BacklogPool
	backlogQueue          chan *txBacklogMsg
	postVerificationQueue chan *txBacklogMsg
	backlogWg             sync.WaitGroup
	net                   network.GossipNode
	ctx                   context.Context
	ctxCancel             context.CancelFunc
}

func MakeTxHandler(txPool *pools.TransactionPool, ledger *Ledger, net network.GossipNode, genesisID string, genesisHash crypto.Digest, executionPool execpool.BacklogPool) *TxHandler {

	if txPool == nil {
		logging.Base().Fatal("MakeTxHandler: txPool is nil on initialization")
		return nil
	}

	if ledger == nil {
		logging.Base().Fatal("MakeTxHandler: ledger is nil on initialization")
		return nil
	}

	handler := &TxHandler{
		txPool:                txPool,
		genesisID:             genesisID,
		genesisHash:           genesisHash,
		ledger:                ledger,
		txVerificationPool:    executionPool,
		backlogQueue:          make(chan *txBacklogMsg, txBacklogSize),
		postVerificationQueue: make(chan *txBacklogMsg, txBacklogSize),
		net:                   net,
	}

	handler.ctx, handler.ctxCancel = context.WithCancel(context.Background())
	return handler
}

func (handler *TxHandler) Start() {
	handler.net.RegisterHandlers([]network.TaggedMessageHandler{
		{Tag: protocol.TxnTag, MessageHandler: network.HandlerFunc(handler.processIncomingTxn)},
	})
	handler.backlogWg.Add(1)
	go handler.backlogWorker()
}

func (handler *TxHandler) Stop() {
	handler.ctxCancel()
	handler.backlogWg.Wait()
}

func reencode(stxns []transactions.SignedTxn) []byte {
	var result [][]byte
	for _, stxn := range stxns {
		result = append(result, protocol.Encode(&stxn))
	}
	return bytes.Join(result, nil)
}

func (handler *TxHandler) backlogWorker() {
	defer handler.backlogWg.Done()
	for {
		select {
		case wi, ok := <-handler.postVerificationQueue:
			if !ok {
				return
			}
			handler.postprocessCheckedTxn(wi)

			continue
		default:
		}

		select {
		case wi, ok := <-handler.backlogQueue:
			if !ok {
				return
			}
			if handler.checkAlreadyCommitted(wi) {
				continue
			}

			handler.txVerificationPool.EnqueueBacklog(handler.ctx, handler.asyncVerifySignature, wi, nil)

		case wi, ok := <-handler.postVerificationQueue:
			if !ok {
				return
			}
			handler.postprocessCheckedTxn(wi)

		case <-handler.ctx.Done():
			return
		}
	}
}

func (handler *TxHandler) postprocessCheckedTxn(wi *txBacklogMsg) {
	if wi.verificationErr != nil {
		logging.Base().Warnf("Received a malformed tx group %v: %v", wi.unverifiedTxGroup, wi.verificationErr)
		handler.net.Disconnect(wi.rawmsg.Sender)
		return
	}

	transactionMessagesHandled.Inc(nil)

	verifiedTxGroup := wi.unverifiedTxGroup

	err := handler.txPool.Remember(verifiedTxGroup)
	if err != nil {
		logging.Base().Debugf("could not remember tx: %v", err)
		return
	}

	err = handler.ledger.VerifiedTransactionCache().Pin(verifiedTxGroup)
	if err != nil {
		logging.Base().Infof("unable to pin transaction: %v", err)
	}

	handler.net.Relay(handler.ctx, protocol.TxnTag, reencode(verifiedTxGroup), false, wi.rawmsg.Sender)
}

func (handler *TxHandler) asyncVerifySignature(arg interface{}) interface{} {
	tx := arg.(*txBacklogMsg)

	latest := handler.ledger.Latest()
	latestHdr, err := handler.ledger.BlockHdr(latest)
	if err != nil {
		tx.verificationErr = fmt.Errorf("Could not get header for previous block %d: %w", latest, err)
		logging.Base().Warnf("Could not get header for previous block %d: %v", latest, err)
	} else {
		_, tx.verificationErr = verify.TxnGroup(tx.unverifiedTxGroup, latestHdr, handler.ledger.VerifiedTransactionCache())
	}

	select {
	case handler.postVerificationQueue <- tx:
	default:
		transactionMessagesDroppedFromPool.Inc(nil)
	}
	return nil
}

func (handler *TxHandler) processIncomingTxn(rawmsg network.IncomingMessage) network.OutgoingMessage {
	dec := protocol.NewDecoderBytes(rawmsg.Data)
	ntx := 0
	unverifiedTxGroup := make([]transactions.SignedTxn, 1)
	for {
		if len(unverifiedTxGroup) == ntx {
			n := make([]transactions.SignedTxn, len(unverifiedTxGroup)*2)
			copy(n, unverifiedTxGroup)
			unverifiedTxGroup = n
		}

		err := dec.Decode(&unverifiedTxGroup[ntx])
		fmt.Printf("%+v\n", unverifiedTxGroup[ntx])
		if err == io.EOF {
			break
		}
		if err != nil {
			logging.Base().Warnf("Received a non-decodable txn: %v", err)
			return network.OutgoingMessage{Action: network.Disconnect}
		}
		ntx++
	}
	if ntx == 0 {
		logging.Base().Warnf("Received empty tx group")
		return network.OutgoingMessage{Action: network.Disconnect}
	}
	unverifiedTxGroup = unverifiedTxGroup[:ntx]

	select {
	case handler.backlogQueue <- &txBacklogMsg{
		rawmsg:            &rawmsg,
		unverifiedTxGroup: unverifiedTxGroup,
	}:
	default:
		transactionMessagesDroppedFromBacklog.Inc(nil)
	}

	return network.OutgoingMessage{Action: network.Ignore}
}

func (handler *TxHandler) checkAlreadyCommitted(tx *txBacklogMsg) (processingDone bool) {
	txids := make([]transactions.Txid, len(tx.unverifiedTxGroup))
	for i := range tx.unverifiedTxGroup {
		txids[i] = tx.unverifiedTxGroup[i].ID()
	}
	logging.Base().Debugf("got a tx group with IDs %v", txids)

	err := handler.txPool.Test(tx.unverifiedTxGroup)
	if err != nil {
		logging.Base().Debugf("txPool rejected transaction: %v", err)
		return true
	}
	return false
}

func (handler *TxHandler) processDecoded(unverifiedTxGroup []transactions.SignedTxn) (outmsg network.OutgoingMessage, processingDone bool) {
	tx := &txBacklogMsg{
		unverifiedTxGroup: unverifiedTxGroup,
	}
	if handler.checkAlreadyCommitted(tx) {
		return network.OutgoingMessage{}, true
	}

	latest := handler.ledger.Latest()
	latestHdr, err := handler.ledger.BlockHdr(latest)
	if err != nil {
		logging.Base().Warnf("Could not get header for previous block %v: %v", latest, err)
		return network.OutgoingMessage{}, true
	}

	unverifiedTxnGroups := bookkeeping.SignedTxnsToGroups(unverifiedTxGroup)
	err = verify.PaysetGroups(context.Background(), unverifiedTxnGroups, latestHdr, handler.txVerificationPool, handler.ledger.VerifiedTransactionCache())
	if err != nil {
		logging.Base().Warnf("One or more transactions were malformed: %v", err)
		return network.OutgoingMessage{Action: network.Disconnect}, true
	}

	verifiedTxGroup := unverifiedTxGroup

	err = handler.txPool.Remember(verifiedTxGroup)
	if err != nil {
		logging.Base().Debugf("could not remember tx: %v", err)
		return network.OutgoingMessage{}, true
	}

	err = handler.ledger.VerifiedTransactionCache().Pin(verifiedTxGroup)
	if err != nil {
		logging.Base().Warnf("unable to pin transaction: %v", err)
	}

	return network.OutgoingMessage{}, false
}

type SolicitedTxHandler interface {
	Handle(txgroup []transactions.SignedTxn) error
}

func (handler *TxHandler) SolicitedTxHandler() SolicitedTxHandler {
	return &solicitedTxHandler{txHandler: handler}
}

type solicitedTxHandler struct {
	txHandler *TxHandler
}

func (handler *solicitedTxHandler) Handle(txgroup []transactions.SignedTxn) error {
	outmsg, _ := handler.txHandler.processDecoded(txgroup)
	if outmsg.Action == network.Disconnect {
		return fmt.Errorf("invlid transaction")
	}
	return nil
}
