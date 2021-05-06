package pools

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/algorand/go-deadlock"

	"github.com/algorand/go-algorand/config"
	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/ledger"
	"github.com/algorand/go-algorand/ledger/ledgercore"
	"github.com/algorand/go-algorand/logging"
	"github.com/algorand/go-algorand/logging/telemetryspec"
	"github.com/algorand/go-algorand/protocol"
	"github.com/algorand/go-algorand/util/condvar"
)

type TransactionPool struct {
	feePerByte uint64

	logProcessBlockStats bool
	logAssembleStats     bool
	expFeeFactor         uint64
	txPoolMaxSize        int
	ledger               *ledger.Ledger

	mu                     deadlock.Mutex
	cond                   sync.Cond
	expiredTxCount         map[basics.Round]int
	pendingBlockEvaluator  *ledger.BlockEvaluator
	numPendingWholeBlocks  basics.Round
	feeThresholdMultiplier uint64
	statusCache            *statusCache

	assemblyMu       deadlock.Mutex
	assemblyCond     sync.Cond
	assemblyDeadline time.Time
	assemblyRound    basics.Round
	assemblyResults  poolAsmResults

	pendingMu       deadlock.RWMutex
	pendingTxGroups [][]transactions.SignedTxn
	pendingTxids    map[transactions.Txid]transactions.SignedTxn

	rememberedTxGroups [][]transactions.SignedTxn
	rememberedTxids    map[transactions.Txid]transactions.SignedTxn

	log logging.Logger
}

func MakeTransactionPool(ledger *ledger.Ledger, cfg config.Local, log logging.Logger) *TransactionPool {
	if cfg.TxPoolExponentialIncreaseFactor < 1 {
		cfg.TxPoolExponentialIncreaseFactor = 1
	}
	pool := TransactionPool{
		pendingTxids:         make(map[transactions.Txid]transactions.SignedTxn),
		rememberedTxids:      make(map[transactions.Txid]transactions.SignedTxn),
		expiredTxCount:       make(map[basics.Round]int),
		ledger:               ledger,
		statusCache:          makeStatusCache(cfg.TxPoolSize),
		logProcessBlockStats: cfg.EnableProcessBlockStats,
		logAssembleStats:     cfg.EnableAssembleStats,
		expFeeFactor:         cfg.TxPoolExponentialIncreaseFactor,
		txPoolMaxSize:        cfg.TxPoolSize,
		log:                  log,
	}
	pool.cond.L = &pool.mu
	pool.assemblyCond.L = &pool.assemblyMu
	pool.recomputeBlockEvaluator(make(map[transactions.Txid]basics.Round), 0)
	return &pool
}

type poolAsmResults struct {
	ok                           bool
	blk                          *ledger.ValidatedBlock
	stats                        telemetryspec.AssembleBlockMetrics
	err                          error
	roundStartedEvaluating       basics.Round
	assemblyCompletedOrAbandoned bool
}

const (
	expiredHistory = 10

	timeoutOnNewBlock = time.Second

	assemblyWaitEps = 150 * time.Millisecond

	generateBlockBaseDuration        = 2 * time.Millisecond
	generateBlockTransactionDuration = 2155 * time.Nanosecond
)

var ErrStaleBlockAssemblyRequest = fmt.Errorf("AssembleBlock: requested block assembly specified a round that is older than current transaction pool round")

func (pool *TransactionPool) Reset() {
	pool.pendingTxids = make(map[transactions.Txid]transactions.SignedTxn)
	pool.pendingTxGroups = nil
	pool.rememberedTxids = make(map[transactions.Txid]transactions.SignedTxn)
	pool.rememberedTxGroups = nil
	pool.expiredTxCount = make(map[basics.Round]int)
	pool.numPendingWholeBlocks = 0
	pool.pendingBlockEvaluator = nil
	pool.statusCache.reset()
	pool.recomputeBlockEvaluator(make(map[transactions.Txid]basics.Round), 0)
}

func (pool *TransactionPool) NumExpired(round basics.Round) int {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return pool.expiredTxCount[round]
}

func (pool *TransactionPool) PendingTxIDs() []transactions.Txid {
	pool.pendingMu.RLock()
	defer pool.pendingMu.RUnlock()

	ids := make([]transactions.Txid, len(pool.pendingTxids))
	i := 0
	for txid := range pool.pendingTxids {
		ids[i] = txid
		i++
	}
	return ids
}

func (pool *TransactionPool) PendingTxGroups() [][]transactions.SignedTxn {
	pool.pendingMu.RLock()
	defer pool.pendingMu.RUnlock()
	return pool.pendingTxGroups
}

func (pool *TransactionPool) pendingTxIDsCount() int {
	pool.pendingMu.RLock()
	defer pool.pendingMu.RUnlock()
	return len(pool.pendingTxids)
}

func (pool *TransactionPool) rememberCommit(flush bool) {
	pool.pendingMu.Lock()
	defer pool.pendingMu.Unlock()

	if flush {
		pool.pendingTxGroups = pool.rememberedTxGroups
		pool.pendingTxids = pool.rememberedTxids
		pool.ledger.VerifiedTransactionCache().UpdatePinned(pool.pendingTxids)
	} else {
		pool.pendingTxGroups = append(pool.pendingTxGroups, pool.rememberedTxGroups...)

		for txid, txn := range pool.rememberedTxids {
			pool.pendingTxids[txid] = txn
		}
	}

	pool.rememberedTxGroups = nil
	pool.rememberedTxids = make(map[transactions.Txid]transactions.SignedTxn)
}

func (pool *TransactionPool) PendingCount() int {
	pool.pendingMu.RLock()
	defer pool.pendingMu.RUnlock()
	return pool.pendingCountNoLock()
}

func (pool *TransactionPool) pendingCountNoLock() int {
	var count int
	for _, txgroup := range pool.pendingTxGroups {
		count += len(txgroup)
	}
	return count
}

func (pool *TransactionPool) checkPendingQueueSize(txCount int) error {
	pendingSize := pool.pendingTxIDsCount()
	if pendingSize+txCount > pool.txPoolMaxSize {
		return fmt.Errorf("TransactionPool.checkPendingQueueSize: transaction pool have reached capacity")
	}
	return nil
}

func (pool *TransactionPool) FeePerByte() uint64 {
	return atomic.LoadUint64(&pool.feePerByte)
}

func (pool *TransactionPool) computeFeePerByte() uint64 {
	feePerByte := uint64(1)

	feePerByte = feePerByte * pool.feeThresholdMultiplier

	if feePerByte == 0 && pool.numPendingWholeBlocks > 1 {
		feePerByte = uint64(1)
	}

	for i := 0; i < int(pool.numPendingWholeBlocks)-1; i++ {
		feePerByte *= pool.expFeeFactor
	}

	atomic.StoreUint64(&pool.feePerByte, feePerByte)

	return feePerByte
}

func (pool *TransactionPool) checkSufficientFee(txgroup []transactions.SignedTxn) error {
	if len(txgroup) == 1 {
		t := txgroup[0].Txn
		if t.Type == protocol.CompactCertTx && t.Sender == transactions.CompactCertSender && t.Fee.IsZero() {
			return nil
		}
	}

	feePerByte := pool.computeFeePerByte()

	for _, t := range txgroup {
		feeThreshold := feePerByte * uint64(t.GetEncodedLength())
		if t.Txn.Fee.Raw < feeThreshold {
			return fmt.Errorf("fee %d below threshold %d (%d per byte * %d bytes)",
				t.Txn.Fee, feeThreshold, feePerByte, t.GetEncodedLength())
		}
	}

	return nil
}

func (pool *TransactionPool) Test(txgroup []transactions.SignedTxn) error {
	fmt.Println("TransactionPool Test", len(txgroup))
	if err := pool.checkPendingQueueSize(len(txgroup)); err != nil {
		return err
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	if pool.pendingBlockEvaluator == nil {
		return fmt.Errorf("Test: pendingBlockEvaluator is nil")
	}

	return pool.pendingBlockEvaluator.TestTransactionGroup(txgroup)
}

type poolIngestParams struct {
	recomputing bool // if unset, perform fee checks and wait until ledger is caught up
	stats       *telemetryspec.AssembleBlockMetrics
}

func (pool *TransactionPool) remember(txgroup []transactions.SignedTxn) error {
	params := poolIngestParams{
		recomputing: false,
	}
	return pool.ingest(txgroup, params)
}

func (pool *TransactionPool) add(txgroup []transactions.SignedTxn, stats *telemetryspec.AssembleBlockMetrics) error {
	params := poolIngestParams{
		recomputing: true,
		stats:       stats,
	}
	return pool.ingest(txgroup, params)
}

func (pool *TransactionPool) ingest(txgroup []transactions.SignedTxn, params poolIngestParams) error {
	if pool.pendingBlockEvaluator == nil {
		return fmt.Errorf("TransactionPool.ingest: no pending block evaluator")
	}

	if !params.recomputing {
		latest := pool.ledger.Latest()
		waitExpires := time.Now().Add(timeoutOnNewBlock)
		for pool.pendingBlockEvaluator.Round() <= latest && time.Now().Before(waitExpires) {
			condvar.TimedWait(&pool.cond, timeoutOnNewBlock)
			if pool.pendingBlockEvaluator == nil {
				return fmt.Errorf("TransactionPool.ingest: no pending block evaluator")
			}
		}

		err := pool.checkSufficientFee(txgroup)
		if err != nil {
			return err
		}
	}

	err := pool.addToPendingBlockEvaluator(txgroup, params.recomputing, params.stats)
	if err != nil {
		return err
	}

	pool.rememberedTxGroups = append(pool.rememberedTxGroups, txgroup)
	for _, t := range txgroup {
		pool.rememberedTxids[t.ID()] = t
	}
	return nil
}

func (pool *TransactionPool) RememberOne(t transactions.SignedTxn) error {
	return pool.Remember([]transactions.SignedTxn{t})
}

func (pool *TransactionPool) Remember(txgroup []transactions.SignedTxn) error {
	if err := pool.checkPendingQueueSize(len(txgroup)); err != nil {
		return err
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	err := pool.remember(txgroup)
	if err != nil {
		return fmt.Errorf("TransactionPool.Remember: %v", err)
	}

	pool.rememberCommit(false)
	return nil
}

func (pool *TransactionPool) Lookup(txid transactions.Txid) (tx transactions.SignedTxn, txErr string, found bool) {
	if pool == nil {
		return transactions.SignedTxn{}, "", false
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()

	pool.pendingMu.RLock()
	defer pool.pendingMu.RUnlock()

	tx, inPool := pool.pendingTxids[txid]
	if inPool {
		return tx, "", true
	}

	return pool.statusCache.check(txid)
}

func (pool *TransactionPool) OnNewBlock(block bookkeeping.Block, delta ledgercore.StateDelta) {
	var stats telemetryspec.ProcessBlockMetrics
	var knownCommitted uint
	var unknownCommitted uint

	committedTxids := delta.Txids
	if pool.logProcessBlockStats {
		pool.pendingMu.RLock()
		for txid := range committedTxids {
			if _, ok := pool.pendingTxids[txid]; ok {
				knownCommitted++
			} else {
				unknownCommitted++
			}
		}
		pool.pendingMu.RUnlock()
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	defer pool.cond.Broadcast()

	if pool.pendingBlockEvaluator == nil || block.Round() >= pool.pendingBlockEvaluator.Round() {
		switch pool.numPendingWholeBlocks {
		case 0:
			pool.feeThresholdMultiplier = pool.feeThresholdMultiplier / pool.expFeeFactor

		case 1:

		default:
			if pool.feeThresholdMultiplier == 0 {
				pool.feeThresholdMultiplier = 1
			} else {
				pool.feeThresholdMultiplier = pool.feeThresholdMultiplier * pool.expFeeFactor
			}
		}

		stats = pool.recomputeBlockEvaluator(committedTxids, knownCommitted)
	}

	stats.KnownCommittedCount = knownCommitted
	stats.UnknownCommittedCount = unknownCommitted

	proto := config.Consensus[block.CurrentProtocol]
	pool.expiredTxCount[block.Round()] = int(stats.ExpiredCount)
	delete(pool.expiredTxCount, block.Round()-expiredHistory*basics.Round(proto.MaxTxnLife))

	if pool.logProcessBlockStats {
		var details struct {
			Round uint64
		}
		details.Round = uint64(block.Round())
		pool.log.Metrics(telemetryspec.Transaction, stats, details)
	}
}

func (pool *TransactionPool) isAssemblyTimedOut() bool {
	if pool.assemblyDeadline.IsZero() {
		return false
	}
	generateBlockDuration := generateBlockBaseDuration + time.Duration(pool.pendingBlockEvaluator.TxnCounter())*generateBlockTransactionDuration
	return time.Now().After(pool.assemblyDeadline.Add(-generateBlockDuration))
}

func (pool *TransactionPool) addToPendingBlockEvaluatorOnce(txgroup []transactions.SignedTxn, recomputing bool, stats *telemetryspec.AssembleBlockMetrics) error {
	r := pool.pendingBlockEvaluator.Round() + pool.numPendingWholeBlocks
	for _, tx := range txgroup {
		if tx.Txn.LastValid < r {
			return transactions.TxnDeadError{
				Round:      r,
				FirstValid: tx.Txn.FirstValid,
				LastValid:  tx.Txn.LastValid,
			}
		}
	}

	txgroupad := make([]transactions.SignedTxnWithAD, len(txgroup))
	for i, tx := range txgroup {
		txgroupad[i].SignedTxn = tx
	}

	transactionGroupStartsTime := time.Time{}
	if recomputing {
		transactionGroupStartsTime = time.Now()
	}

	err := pool.pendingBlockEvaluator.TransactionGroup(txgroupad)

	if recomputing {
		if !pool.assemblyResults.assemblyCompletedOrAbandoned {
			transactionGroupDuration := time.Now().Sub(transactionGroupStartsTime)
			pool.assemblyMu.Lock()
			defer pool.assemblyMu.Unlock()
			if pool.assemblyRound > pool.pendingBlockEvaluator.Round() {
				pool.assemblyResults.ok = true
				pool.assemblyResults.assemblyCompletedOrAbandoned = true
				stats.StopReason = telemetryspec.AssembleBlockAbandon
				pool.assemblyResults.stats = *stats
				pool.assemblyCond.Broadcast()
			} else if err == ledger.ErrNoSpace || pool.isAssemblyTimedOut() {
				pool.assemblyResults.ok = true
				pool.assemblyResults.assemblyCompletedOrAbandoned = true
				if err == ledger.ErrNoSpace {
					stats.StopReason = telemetryspec.AssembleBlockFull
				} else {
					stats.StopReason = telemetryspec.AssembleBlockTimeout
					stats.ProcessingTime.AddTransaction(transactionGroupDuration)
				}

				blockGenerationStarts := time.Now()
				lvb, gerr := pool.pendingBlockEvaluator.GenerateBlock()
				if gerr != nil {
					pool.assemblyResults.err = fmt.Errorf("could not generate block for %d: %v", pool.assemblyResults.roundStartedEvaluating, gerr)
				} else {
					pool.assemblyResults.blk = lvb
				}
				stats.BlockGenerationDuration = uint64(time.Now().Sub(blockGenerationStarts))
				pool.assemblyResults.stats = *stats
				pool.assemblyCond.Broadcast()
			} else {
				stats.ProcessingTime.AddTransaction(transactionGroupDuration)
			}
		}
	}
	return err
}

func (pool *TransactionPool) addToPendingBlockEvaluator(txgroup []transactions.SignedTxn, recomputing bool, stats *telemetryspec.AssembleBlockMetrics) error {
	err := pool.addToPendingBlockEvaluatorOnce(txgroup, recomputing, stats)
	if err == ledger.ErrNoSpace {
		pool.numPendingWholeBlocks++
		pool.pendingBlockEvaluator.ResetTxnBytes()
		err = pool.addToPendingBlockEvaluatorOnce(txgroup, recomputing, stats)
	}
	return err
}

func (pool *TransactionPool) recomputeBlockEvaluator(committedTxIds map[transactions.Txid]basics.Round, knownCommitted uint) (stats telemetryspec.ProcessBlockMetrics) {
	pool.pendingBlockEvaluator = nil

	latest := pool.ledger.Latest()
	prev, err := pool.ledger.BlockHdr(latest)
	if err != nil {
		pool.log.Warnf("TransactionPool.recomputeBlockEvaluator: cannot get prev header for %d: %v",
			latest, err)
		return
	}

	_, upgradeState, err := bookkeeping.ProcessUpgradeParams(prev)
	if err != nil {
		pool.log.Warnf("TransactionPool.recomputeBlockEvaluator: error processing upgrade params for next round: %v", err)
		return
	}

	_, ok := config.Consensus[upgradeState.CurrentProtocol]
	if !ok {
		pool.log.Warnf("TransactionPool.recomputeBlockEvaluator: next protocol version %v is not supported", upgradeState.CurrentProtocol)
		return
	}

	pool.pendingMu.RLock()
	txgroups := pool.pendingTxGroups
	pendingCount := pool.pendingCountNoLock()
	pool.pendingMu.RUnlock()

	pool.assemblyMu.Lock()
	pool.assemblyResults = poolAsmResults{
		roundStartedEvaluating: prev.Round + basics.Round(1),
	}
	pool.assemblyMu.Unlock()

	next := bookkeeping.MakeBlock(prev)
	pool.numPendingWholeBlocks = 0
	hint := pendingCount - int(knownCommitted)
	if hint < 0 || int(knownCommitted) < 0 {
		hint = 0
	}
	pool.pendingBlockEvaluator, err = pool.ledger.StartEvaluator(next.BlockHeader, hint)
	if err != nil {
		pool.log.Warnf("TransactionPool.recomputeBlockEvaluator: cannot start evaluator: %v", err)
		return
	}

	var asmStats telemetryspec.AssembleBlockMetrics
	asmStats.StartCount = len(txgroups)
	asmStats.StopReason = telemetryspec.AssembleBlockEmpty

	firstTxnGrpTime := time.Now()

	for _, txgroup := range txgroups {
		if len(txgroup) == 0 {
			asmStats.InvalidCount++
			continue
		}
		if _, alreadyCommitted := committedTxIds[txgroup[0].ID()]; alreadyCommitted {
			asmStats.EarlyCommittedCount++
			continue
		}
		err := pool.add(txgroup, &asmStats)
		if err != nil {
			for _, tx := range txgroup {
				pool.statusCache.put(tx, err.Error())
			}

			switch err.(type) {
			case ledgercore.TransactionInLedgerError:
				asmStats.CommittedCount++
				stats.RemovedInvalidCount++
			case transactions.TxnDeadError:
				asmStats.InvalidCount++
				stats.ExpiredCount++
			case transactions.MinFeeError:
				asmStats.InvalidCount++
				stats.RemovedInvalidCount++
				pool.log.Infof("Cannot re-add pending transaction to pool: %v", err)
			default:
				asmStats.InvalidCount++
				stats.RemovedInvalidCount++
				pool.log.Warnf("Cannot re-add pending transaction to pool: %v", err)
			}
		}
	}

	pool.assemblyMu.Lock()
	if !pool.assemblyDeadline.IsZero() {
		asmStats.TransactionsLoopStartTime = int64(firstTxnGrpTime.Sub(pool.assemblyDeadline.Add(-config.ProposalAssemblyTime)))
	}

	if !pool.assemblyResults.ok && pool.assemblyRound <= pool.pendingBlockEvaluator.Round() {
		pool.assemblyResults.ok = true
		pool.assemblyResults.assemblyCompletedOrAbandoned = true // this is not strictly needed, since the value would only get inspected by this go-routine, but we'll adjust it along with "ok" for consistency
		blockGenerationStarts := time.Now()
		lvb, err := pool.pendingBlockEvaluator.GenerateBlock()
		if err != nil {
			pool.assemblyResults.err = fmt.Errorf("could not generate block for %d (end): %v", pool.assemblyResults.roundStartedEvaluating, err)
		} else {
			pool.assemblyResults.blk = lvb
		}
		asmStats.BlockGenerationDuration = uint64(time.Now().Sub(blockGenerationStarts))
		pool.assemblyResults.stats = asmStats
		pool.assemblyCond.Broadcast()
	}
	pool.assemblyMu.Unlock()

	pool.rememberCommit(true)
	return
}

func (pool *TransactionPool) AssembleBlock(round basics.Round, deadline time.Time) (assembled *ledger.ValidatedBlock, err error) {
	var stats telemetryspec.AssembleBlockMetrics

	if pool.logAssembleStats {
		start := time.Now()
		defer func() {
			if err != nil {
				return
			}

			dt := time.Now().Sub(start)
			stats.Nanoseconds = dt.Nanoseconds()

			payset := assembled.Block().Payset
			if len(payset) != 0 {
				totalFees := uint64(0)

				for i, txib := range payset {
					fee := txib.Txn.Fee.Raw
					encodedLen := txib.GetEncodedLength()

					stats.IncludedCount++
					totalFees += fee

					if i == 0 {
						stats.MinFee = fee
						stats.MaxFee = fee
						stats.MinLength = encodedLen
						stats.MaxLength = encodedLen
					} else {
						if fee < stats.MinFee {
							stats.MinFee = fee
						} else if fee > stats.MaxFee {
							stats.MaxFee = fee
						}
						if encodedLen < stats.MinLength {
							stats.MinLength = encodedLen
						} else if encodedLen > stats.MaxLength {
							stats.MaxLength = encodedLen
						}
					}
					stats.TotalLength += uint64(encodedLen)
				}

				stats.AverageFee = totalFees / uint64(stats.IncludedCount)
			}

			var details struct {
				Round uint64
			}
			details.Round = uint64(round)
			pool.log.Metrics(telemetryspec.Transaction, stats, details)
		}()
	}

	pool.assemblyMu.Lock()

	if pool.assemblyResults.roundStartedEvaluating <= round.SubSaturate(2) {
		pool.log.Infof("AssembleBlock: requested round is more than a single round ahead of the transaction pool %d <= %d-2", pool.assemblyResults.roundStartedEvaluating, round)
		stats.StopReason = telemetryspec.AssembleBlockEmpty
		pool.assemblyMu.Unlock()
		return pool.assembleEmptyBlock(round)
	}

	defer pool.assemblyMu.Unlock()

	if pool.assemblyResults.roundStartedEvaluating > round {
		pool.log.Infof("AssembleBlock: requested round is behind transaction pool round %d < %d", round, pool.assemblyResults.roundStartedEvaluating)
		return nil, ErrStaleBlockAssemblyRequest
	}

	pool.assemblyDeadline = deadline
	pool.assemblyRound = round
	for time.Now().Before(deadline) && (!pool.assemblyResults.ok || pool.assemblyResults.roundStartedEvaluating != round) {
		condvar.TimedWait(&pool.assemblyCond, deadline.Sub(time.Now()))
	}

	if !pool.assemblyResults.ok {
		pool.assemblyMu.Unlock()
		emptyBlock, emptyBlockErr := pool.assembleEmptyBlock(round)
		pool.assemblyMu.Lock()

		if pool.assemblyResults.roundStartedEvaluating > round {
			pool.log.Infof("AssembleBlock: requested round is behind transaction pool round after timing out %d < %d", round, pool.assemblyResults.roundStartedEvaluating)
			return nil, ErrStaleBlockAssemblyRequest
		}

		deadline = deadline.Add(assemblyWaitEps)
		for time.Now().Before(deadline) && (!pool.assemblyResults.ok || pool.assemblyResults.roundStartedEvaluating != round) {
			condvar.TimedWait(&pool.assemblyCond, deadline.Sub(time.Now()))
		}

		if !pool.assemblyResults.ok {
			pool.log.Warnf("AssembleBlock: ran out of time for round %d", round)
			stats.StopReason = telemetryspec.AssembleBlockTimeout
			if emptyBlockErr != nil {
				emptyBlockErr = fmt.Errorf("AssembleBlock: failed to construct empty block : %v", emptyBlockErr)
			}
			return emptyBlock, emptyBlockErr
		}
	}
	pool.assemblyDeadline = time.Time{}

	if pool.assemblyResults.err != nil {
		return nil, fmt.Errorf("AssemblyBlock: encountered error for round %d: %v", round, pool.assemblyResults.err)
	}
	if pool.assemblyResults.roundStartedEvaluating > round {
		pool.log.Warnf("AssembleBlock: requested round is behind transaction pool round %d < %d", round, pool.assemblyResults.roundStartedEvaluating)
		return nil, ErrStaleBlockAssemblyRequest
	} else if pool.assemblyResults.roundStartedEvaluating == round.SubSaturate(1) {
		pool.log.Warnf("AssembleBlock: assembled block round did not catch up to requested round: %d != %d", pool.assemblyResults.roundStartedEvaluating, round)
		stats.StopReason = telemetryspec.AssembleBlockTimeout
		return pool.assembleEmptyBlock(round)
	} else if pool.assemblyResults.roundStartedEvaluating < round {
		return nil, fmt.Errorf("AssembleBlock: assembled block round much behind requested round: %d != %d",
			pool.assemblyResults.roundStartedEvaluating, round)
	}

	stats = pool.assemblyResults.stats
	return pool.assemblyResults.blk, nil
}

func (pool *TransactionPool) assembleEmptyBlock(round basics.Round) (assembled *ledger.ValidatedBlock, err error) {
	prevRound := round - 1
	prev, err := pool.ledger.BlockHdr(prevRound)
	if err != nil {
		err = fmt.Errorf("TransactionPool.assembleEmptyBlock: cannot get prev header for %d: %v", prevRound, err)
		return nil, err
	}
	next := bookkeeping.MakeBlock(prev)
	blockEval, err := pool.ledger.StartEvaluator(next.BlockHeader, 0)
	if err != nil {
		err = fmt.Errorf("TransactionPool.assembleEmptyBlock: cannot start evaluator for %d: %v", round, err)
		return nil, err
	}
	return blockEval.GenerateBlock()
}
