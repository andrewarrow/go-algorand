package ledger

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/algorand/go-algorand/config"
	"github.com/algorand/go-algorand/crypto"
	"github.com/algorand/go-algorand/crypto/compactcert"
	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/committee"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/data/transactions/logic"
	"github.com/algorand/go-algorand/data/transactions/verify"
	"github.com/algorand/go-algorand/ledger/apply"
	"github.com/algorand/go-algorand/ledger/ledgercore"
	"github.com/algorand/go-algorand/logging"
	"github.com/algorand/go-algorand/protocol"
	"github.com/algorand/go-algorand/util/execpool"
)

var ErrNoSpace = errors.New("block does not have space for transaction")

const maxPaysetHint = 20000

const asyncAccountLoadingThreadCount = 4

type roundCowBase struct {
	l ledgerForCowBase

	rnd basics.Round

	txnCount uint64

	compactCertNextRnd basics.Round

	proto config.ConsensusParams

	accounts map[basics.Address]basics.AccountData
}

func (x *roundCowBase) getCreator(cidx basics.CreatableIndex, ctype basics.CreatableType) (basics.Address, bool, error) {
	return x.l.GetCreatorForRound(x.rnd, cidx, ctype)
}

func (x *roundCowBase) lookup(addr basics.Address) (basics.AccountData, error) {
	if accountData, found := x.accounts[addr]; found {
		return accountData, nil
	}

	accountData, _, err := x.l.LookupWithoutRewards(x.rnd, addr)
	if err == nil {
		x.accounts[addr] = accountData
	}
	return accountData, err
}

func (x *roundCowBase) checkDup(firstValid, lastValid basics.Round, txid transactions.Txid, txl ledgercore.Txlease) error {
	return x.l.CheckDup(x.proto, x.rnd+1, firstValid, lastValid, txid, TxLease{txl})
}

func (x *roundCowBase) txnCounter() uint64 {
	return x.txnCount
}

func (x *roundCowBase) compactCertNext() basics.Round {
	return x.compactCertNextRnd
}

func (x *roundCowBase) blockHdr(r basics.Round) (bookkeeping.BlockHeader, error) {
	return x.l.BlockHdr(r)
}

func (x *roundCowBase) allocated(addr basics.Address, aidx basics.AppIndex, global bool) (bool, error) {
	acct, _, err := x.l.LookupWithoutRewards(x.rnd, addr)
	if err != nil {
		return false, err
	}

	if global {
		_, ok := acct.AppParams[aidx]
		return ok, nil
	}

	_, ok := acct.AppLocalStates[aidx]
	return ok, nil
}

func (x *roundCowBase) getKey(addr basics.Address, aidx basics.AppIndex, global bool, key string, accountIdx uint64) (basics.TealValue, bool, error) {
	ad, _, err := x.l.LookupWithoutRewards(x.rnd, addr)
	if err != nil {
		return basics.TealValue{}, false, err
	}

	exist := false
	kv := basics.TealKeyValue{}
	if global {
		var app basics.AppParams
		if app, exist = ad.AppParams[aidx]; exist {
			kv = app.GlobalState
		}
	} else {
		var ls basics.AppLocalState
		if ls, exist = ad.AppLocalStates[aidx]; exist {
			kv = ls.KeyValue
		}
	}
	if !exist {
		err = fmt.Errorf("cannot fetch key, %v", errNoStorage(addr, aidx, global))
		return basics.TealValue{}, false, err
	}

	val, exist := kv[key]
	return val, exist, nil
}

func (x *roundCowBase) getStorageCounts(addr basics.Address, aidx basics.AppIndex, global bool) (basics.StateSchema, error) {
	ad, _, err := x.l.LookupWithoutRewards(x.rnd, addr)
	if err != nil {
		return basics.StateSchema{}, err
	}

	count := basics.StateSchema{}
	exist := false
	kv := basics.TealKeyValue{}
	if global {
		var app basics.AppParams
		if app, exist = ad.AppParams[aidx]; exist {
			kv = app.GlobalState
		}
	} else {
		var ls basics.AppLocalState
		if ls, exist = ad.AppLocalStates[aidx]; exist {
			kv = ls.KeyValue
		}
	}
	if !exist {
		return count, nil
	}

	for _, v := range kv {
		if v.Type == basics.TealUintType {
			count.NumUint++
		} else {
			count.NumByteSlice++
		}
	}
	return count, nil
}

func (x *roundCowBase) getStorageLimits(addr basics.Address, aidx basics.AppIndex, global bool) (basics.StateSchema, error) {
	creator, exists, err := x.getCreator(basics.CreatableIndex(aidx), basics.AppCreatable)
	if err != nil {
		return basics.StateSchema{}, err
	}

	if !exists {
		return basics.StateSchema{}, nil
	}

	record, err := x.lookup(creator)
	if err != nil {
		return basics.StateSchema{}, err
	}

	params, ok := record.AppParams[aidx]
	if !ok {
		err = fmt.Errorf("app %d not found in account %s", aidx, creator.String())
		return basics.StateSchema{}, err
	}

	if global {
		return params.GlobalStateSchema, nil
	}
	return params.LocalStateSchema, nil
}

func (cs *roundCowState) Get(addr basics.Address, withPendingRewards bool) (basics.AccountData, error) {
	acct, err := cs.lookup(addr)
	if err != nil {
		return basics.AccountData{}, err
	}
	if withPendingRewards {
		acct = acct.WithUpdatedRewards(cs.proto, cs.rewardsLevel())
	}
	return acct, nil
}

func (cs *roundCowState) GetCreator(cidx basics.CreatableIndex, ctype basics.CreatableType) (basics.Address, bool, error) {
	return cs.getCreator(cidx, ctype)
}

func (cs *roundCowState) Put(addr basics.Address, acct basics.AccountData) error {
	return cs.PutWithCreatable(addr, acct, nil, nil)
}

func (cs *roundCowState) PutWithCreatable(addr basics.Address, acct basics.AccountData, newCreatable *basics.CreatableLocator, deletedCreatable *basics.CreatableLocator) error {
	cs.put(addr, acct, newCreatable, deletedCreatable)
	return nil
}

func (cs *roundCowState) Move(from basics.Address, to basics.Address, amt basics.MicroAlgos, fromRewards *basics.MicroAlgos, toRewards *basics.MicroAlgos) error {
	rewardlvl := cs.rewardsLevel()

	fromBal, err := cs.lookup(from)
	if err != nil {
		return err
	}
	fromBalNew := fromBal.WithUpdatedRewards(cs.proto, rewardlvl)

	if fromRewards != nil {
		var ot basics.OverflowTracker
		newFromRewards := ot.AddA(*fromRewards, ot.SubA(fromBalNew.MicroAlgos, fromBal.MicroAlgos))
		if ot.Overflowed {
			return fmt.Errorf("overflowed tracking of fromRewards for account %v: %d + (%d - %d)", from, *fromRewards, fromBalNew.MicroAlgos, fromBal.MicroAlgos)
		}
		*fromRewards = newFromRewards
	}

	var overflowed bool
	fromBalNew.MicroAlgos, overflowed = basics.OSubA(fromBalNew.MicroAlgos, amt)
	if overflowed {
		return fmt.Errorf("overspend (account %v, data %+v, tried to spend %v)", from, fromBal, amt)
	}
	cs.put(from, fromBalNew, nil, nil)

	toBal, err := cs.lookup(to)
	if err != nil {
		return err
	}
	toBalNew := toBal.WithUpdatedRewards(cs.proto, rewardlvl)

	if toRewards != nil {
		var ot basics.OverflowTracker
		newToRewards := ot.AddA(*toRewards, ot.SubA(toBalNew.MicroAlgos, toBal.MicroAlgos))
		if ot.Overflowed {
			return fmt.Errorf("overflowed tracking of toRewards for account %v: %d + (%d - %d)", to, *toRewards, toBalNew.MicroAlgos, toBal.MicroAlgos)
		}
		*toRewards = newToRewards
	}

	toBalNew.MicroAlgos, overflowed = basics.OAddA(toBalNew.MicroAlgos, amt)
	if overflowed {
		return fmt.Errorf("balance overflow (account %v, data %+v, was going to receive %v)", to, toBal, amt)
	}
	cs.put(to, toBalNew, nil, nil)

	return nil
}

func (cs *roundCowState) ConsensusParams() config.ConsensusParams {
	return cs.proto
}

func (cs *roundCowState) compactCert(certRnd basics.Round, certType protocol.CompactCertType, cert compactcert.Cert, atRound basics.Round, validate bool) error {
	if certType != protocol.CompactCertBasic {
		return fmt.Errorf("compact cert type %d not supported", certType)
	}

	nextCertRnd := cs.compactCertNext()

	certHdr, err := cs.blockHdr(certRnd)
	if err != nil {
		return err
	}

	proto := config.Consensus[certHdr.CurrentProtocol]

	if validate {
		votersRnd := certRnd.SubSaturate(basics.Round(proto.CompactCertRounds))
		votersHdr, err := cs.blockHdr(votersRnd)
		if err != nil {
			return err
		}

		err = validateCompactCert(certHdr, cert, votersHdr, nextCertRnd, atRound)
		if err != nil {
			return err
		}
	}

	cs.setCompactCertNext(certRnd + basics.Round(proto.CompactCertRounds))
	return nil
}

type BlockEvaluator struct {
	state    *roundCowState
	validate bool
	generate bool

	prevHeader  bookkeeping.BlockHeader // cached
	proto       config.ConsensusParams
	genesisHash crypto.Digest

	block        bookkeeping.Block
	blockTxBytes int

	blockGenerated bool // prevent repeated GenerateBlock calls

	l ledgerForEvaluator
}

type ledgerForEvaluator interface {
	ledgerForCowBase
	GenesisHash() crypto.Digest
	Totals(basics.Round) (ledgercore.AccountTotals, error)
	CompactCertVoters(basics.Round) (*VotersForRound, error)
}

type ledgerForCowBase interface {
	BlockHdr(basics.Round) (bookkeeping.BlockHeader, error)
	CheckDup(config.ConsensusParams, basics.Round, basics.Round, basics.Round, transactions.Txid, TxLease) error
	LookupWithoutRewards(basics.Round, basics.Address) (basics.AccountData, basics.Round, error)
	GetCreatorForRound(basics.Round, basics.CreatableIndex, basics.CreatableType) (basics.Address, bool, error)
}

func (l *Ledger) StartEvaluator(hdr bookkeeping.BlockHeader, paysetHint int) (*BlockEvaluator, error) {
	return startEvaluator(l, hdr, paysetHint, true, true)
}

func startEvaluator(l ledgerForEvaluator, hdr bookkeeping.BlockHeader, paysetHint int, validate bool, generate bool) (*BlockEvaluator, error) {
	proto, ok := config.Consensus[hdr.CurrentProtocol]
	if !ok {
		return nil, protocol.Error(hdr.CurrentProtocol)
	}

	base := &roundCowBase{
		l:        l,
		rnd:      hdr.Round - 1,
		proto:    proto,
		accounts: make(map[basics.Address]basics.AccountData),
	}

	eval := &BlockEvaluator{
		validate:    validate,
		generate:    generate,
		block:       bookkeeping.Block{BlockHeader: hdr},
		proto:       proto,
		genesisHash: l.GenesisHash(),
		l:           l,
	}

	if paysetHint > 0 {
		if paysetHint > maxPaysetHint {
			paysetHint = maxPaysetHint
		}
		eval.block.Payset = make([]transactions.SignedTxnInBlock, 0, paysetHint)
	}

	prevProto := proto

	if hdr.Round > 0 {
		var err error
		eval.prevHeader, err = l.BlockHdr(base.rnd)
		if err != nil {
			return nil, fmt.Errorf("can't evaluate block %v without previous header: %v", hdr.Round, err)
		}

		base.txnCount = eval.prevHeader.TxnCounter
		base.compactCertNextRnd = eval.prevHeader.CompactCert[protocol.CompactCertBasic].CompactCertNextRound
		prevProto, ok = config.Consensus[eval.prevHeader.CurrentProtocol]
		if !ok {
			return nil, protocol.Error(eval.prevHeader.CurrentProtocol)
		}

		if base.compactCertNextRnd == 0 && proto.CompactCertRounds != 0 {
			votersRound := (hdr.Round + basics.Round(proto.CompactCertVotersLookback)).RoundUpToMultipleOf(basics.Round(proto.CompactCertRounds))

			base.compactCertNextRnd = votersRound + basics.Round(proto.CompactCertRounds)
		}
	}

	prevTotals, err := l.Totals(eval.prevHeader.Round)
	if err != nil {
		return nil, err
	}

	poolAddr := eval.prevHeader.RewardsPool
	incentivePoolData, _, err := l.LookupWithoutRewards(eval.prevHeader.Round, poolAddr)
	if err != nil {
		return nil, err
	}

	incentivePoolData = incentivePoolData.WithUpdatedRewards(prevProto, eval.prevHeader.RewardsLevel)

	if generate {
		if eval.proto.SupportGenesisHash {
			eval.block.BlockHeader.GenesisHash = eval.genesisHash
		}
		eval.block.BlockHeader.RewardsState = eval.prevHeader.NextRewardsState(hdr.Round, proto, incentivePoolData.MicroAlgos, prevTotals.RewardUnits())
	}
	eval.state = makeRoundCowState(base, eval.block.BlockHeader, eval.prevHeader.TimeStamp, paysetHint)

	if validate {
		err := eval.block.BlockHeader.PreCheck(eval.prevHeader)
		if err != nil {
			return nil, err
		}

		expectedRewardsState := eval.prevHeader.NextRewardsState(hdr.Round, proto, incentivePoolData.MicroAlgos, prevTotals.RewardUnits())
		if eval.block.RewardsState != expectedRewardsState {
			return nil, fmt.Errorf("bad rewards state: %+v != %+v", eval.block.RewardsState, expectedRewardsState)
		}

		if eval.proto.SupportGenesisHash && eval.block.BlockHeader.GenesisHash != eval.genesisHash {
			return nil, fmt.Errorf("wrong genesis hash: %s != %s", eval.block.BlockHeader.GenesisHash, eval.genesisHash)
		}
	}

	var ot basics.OverflowTracker
	rewardsPerUnit := ot.Sub(eval.block.BlockHeader.RewardsLevel, eval.prevHeader.RewardsLevel)
	if ot.Overflowed {
		return nil, fmt.Errorf("overflowed subtracting rewards(%d, %d) levels for block %v", eval.block.BlockHeader.RewardsLevel, eval.prevHeader.RewardsLevel, hdr.Round)
	}

	poolOld, err := eval.state.Get(poolAddr, true)
	if err != nil {
		return nil, err
	}

	poolOld, err = eval.workaroundOverspentRewards(poolOld, hdr.Round)
	if err != nil {
		return nil, err
	}

	poolNew := poolOld
	poolNew.MicroAlgos = ot.SubA(poolOld.MicroAlgos, basics.MicroAlgos{Raw: ot.Mul(prevTotals.RewardUnits(), rewardsPerUnit)})
	if ot.Overflowed {
		return nil, fmt.Errorf("overflowed subtracting reward unit for block %v", hdr.Round)
	}

	err = eval.state.Put(poolAddr, poolNew)
	if err != nil {
		return nil, err
	}

	ot.SubA(poolNew.MicroAlgos, basics.MicroAlgos{Raw: proto.MinBalance})
	if ot.Overflowed {
		return nil, fmt.Errorf("overflowed subtracting rewards for block %v", hdr.Round)
	}

	return eval, nil
}

func (eval *BlockEvaluator) workaroundOverspentRewards(rewardPoolBalance basics.AccountData, headerRound basics.Round) (poolOld basics.AccountData, err error) {
	if headerRound != 1499995 && headerRound != 2926564 {
		return rewardPoolBalance, nil
	}
	testnetGenesisHash, _ := crypto.DigestFromString("JBR3KGFEWPEE5SAQ6IWU6EEBZMHXD4CZU6WCBXWGF57XBZIJHIRA")
	if eval.genesisHash != testnetGenesisHash {
		return rewardPoolBalance, nil
	}

	bankAddr, _ := basics.UnmarshalChecksumAddress("GD64YIY3TWGDMCNPP553DZPPR6LDUSFQOIJVFDPPXWEG3FVOJCCDBBHU5A")
	amount := basics.MicroAlgos{Raw: 20000000000}
	err = eval.state.Move(bankAddr, eval.prevHeader.RewardsPool, amount, nil, nil)
	if err != nil {
		err = fmt.Errorf("unable to move funds from testnet bank to incentive pool: %v", err)
		return
	}
	poolOld, err = eval.state.Get(eval.prevHeader.RewardsPool, true)

	return
}

func (eval *BlockEvaluator) TxnCounter() int {
	return len(eval.block.Payset)
}

func (eval *BlockEvaluator) Round() basics.Round {
	return eval.block.Round()
}

func (eval *BlockEvaluator) ResetTxnBytes() {
	eval.blockTxBytes = 0
}

func (eval *BlockEvaluator) TestTransactionGroup(txgroup []transactions.SignedTxn) error {
	if len(txgroup) == 0 {
		return nil
	}

	if len(txgroup) > eval.proto.MaxTxGroupSize {
		return fmt.Errorf("group size %d exceeds maximum %d", len(txgroup), eval.proto.MaxTxGroupSize)
	}

	fmt.Printf("eval.state.child %+v\n", eval.state)
	cow := eval.state.child(len(txgroup))
	fmt.Printf("eval.state.child %+v\n", cow)

	var group transactions.TxGroup
	for gi, txn := range txgroup {
		err := eval.testTransaction(txn, cow)
		if err != nil {
			return err
		}

		if txn.Txn.Group != txgroup[0].Txn.Group {
			return fmt.Errorf("transactionGroup: inconsistent group values: %v != %v",
				txn.Txn.Group, txgroup[0].Txn.Group)
		}

		if !txn.Txn.Group.IsZero() {
			txWithoutGroup := txn.Txn
			txWithoutGroup.Group = crypto.Digest{}

			group.TxGroupHashes = append(group.TxGroupHashes, crypto.HashObj(txWithoutGroup))
		} else if len(txgroup) > 1 {
			return fmt.Errorf("transactionGroup: [%d] had zero Group but was submitted in a group of %d", gi, len(txgroup))
		}
	}

	if group.TxGroupHashes != nil {
		if txgroup[0].Txn.Group != crypto.HashObj(group) {
			return fmt.Errorf("transactionGroup: incomplete group: %v != %v (%v)",
				txgroup[0].Txn.Group, crypto.HashObj(group), group)
		}
	}

	return nil
}

func (eval *BlockEvaluator) testTransaction(txn transactions.SignedTxn, cow *roundCowState) error {
	err := txn.Txn.Alive(eval.block)
	if err != nil {
		return err
	}

	spec := transactions.SpecialAddresses{
		FeeSink:     eval.block.BlockHeader.FeeSink,
		RewardsPool: eval.block.BlockHeader.RewardsPool,
	}
	err = txn.Txn.WellFormed(spec, eval.proto)
	if err != nil {
		return fmt.Errorf("transaction %v: malformed: %v", txn.ID(), err)
	}

	txid := txn.ID()
	err = cow.checkDup(txn.Txn.First(), txn.Txn.Last(), txid, ledgercore.Txlease{Sender: txn.Txn.Sender, Lease: txn.Txn.Lease})
	if err != nil {
		return err
	}

	return nil
}

func (eval *BlockEvaluator) Transaction(txn transactions.SignedTxn, ad transactions.ApplyData) error {
	return eval.transactionGroup([]transactions.SignedTxnWithAD{
		{
			SignedTxn: txn,
			ApplyData: ad,
		},
	})
}

func (eval *BlockEvaluator) TransactionGroup(txads []transactions.SignedTxnWithAD) error {
	return eval.transactionGroup(txads)
}

func (eval *BlockEvaluator) prepareEvalParams(txgroup []transactions.SignedTxnWithAD) (res []*logic.EvalParams) {
	var groupNoAD []transactions.SignedTxn
	var minTealVersion uint64
	res = make([]*logic.EvalParams, len(txgroup))
	for i, txn := range txgroup {
		if txn.SignedTxn.Txn.Type != protocol.ApplicationCallTx {
			continue
		}

		if groupNoAD == nil {
			groupNoAD = make([]transactions.SignedTxn, len(txgroup))
			for j := range txgroup {
				groupNoAD[j] = txgroup[j].SignedTxn
			}
			minTealVersion = logic.ComputeMinTealVersion(groupNoAD)
		}

		res[i] = &logic.EvalParams{
			Txn:            &groupNoAD[i],
			Proto:          &eval.proto,
			TxnGroup:       groupNoAD,
			GroupIndex:     i,
			MinTealVersion: &minTealVersion,
		}
	}
	return
}

func (eval *BlockEvaluator) transactionGroup(txgroup []transactions.SignedTxnWithAD) error {
	if len(txgroup) == 0 {
		return nil
	}

	if len(txgroup) > eval.proto.MaxTxGroupSize {
		return fmt.Errorf("group size %d exceeds maximum %d", len(txgroup), eval.proto.MaxTxGroupSize)
	}

	var txibs []transactions.SignedTxnInBlock
	var group transactions.TxGroup
	var groupTxBytes int

	cow := eval.state.child(len(txgroup))

	evalParams := eval.prepareEvalParams(txgroup)

	txibs = make([]transactions.SignedTxnInBlock, 0, len(txgroup))
	for gi, txad := range txgroup {
		var txib transactions.SignedTxnInBlock

		err := eval.transaction(txad.SignedTxn, evalParams[gi], txad.ApplyData, cow, &txib)
		if err != nil {
			return err
		}

		txibs = append(txibs, txib)

		if eval.validate {
			groupTxBytes += txib.GetEncodedLength()
			if eval.blockTxBytes+groupTxBytes > eval.proto.MaxTxnBytesPerBlock {
				return ErrNoSpace
			}
		}

		if txad.SignedTxn.Txn.Group != txgroup[0].SignedTxn.Txn.Group {
			return fmt.Errorf("transactionGroup: inconsistent group values: %v != %v",
				txad.SignedTxn.Txn.Group, txgroup[0].SignedTxn.Txn.Group)
		}

		if !txad.SignedTxn.Txn.Group.IsZero() {
			txWithoutGroup := txad.SignedTxn.Txn
			txWithoutGroup.Group = crypto.Digest{}

			group.TxGroupHashes = append(group.TxGroupHashes, crypto.HashObj(txWithoutGroup))
		} else if len(txgroup) > 1 {
			return fmt.Errorf("transactionGroup: [%d] had zero Group but was submitted in a group of %d", gi, len(txgroup))
		}
	}

	if group.TxGroupHashes != nil {
		if txgroup[0].SignedTxn.Txn.Group != crypto.HashObj(group) {
			return fmt.Errorf("transactionGroup: incomplete group: %v != %v (%v)",
				txgroup[0].SignedTxn.Txn.Group, crypto.HashObj(group), group)
		}
	}

	eval.block.Payset = append(eval.block.Payset, txibs...)
	eval.blockTxBytes += groupTxBytes
	cow.commitToParent()

	return nil
}

func (eval *BlockEvaluator) transaction(txn transactions.SignedTxn, evalParams *logic.EvalParams, ad transactions.ApplyData, cow *roundCowState, txib *transactions.SignedTxnInBlock) error {
	var err error

	txid := txn.ID()

	if eval.validate {
		err = txn.Txn.Alive(eval.block)
		if err != nil {
			return err
		}

		err := cow.checkDup(txn.Txn.First(), txn.Txn.Last(), txid, ledgercore.Txlease{Sender: txn.Txn.Sender, Lease: txn.Txn.Lease})
		if err != nil {
			return err
		}

		acctdata, err := cow.lookup(txn.Txn.Sender)
		if err != nil {
			return err
		}
		correctAuthorizer := acctdata.AuthAddr
		if (correctAuthorizer == basics.Address{}) {
			correctAuthorizer = txn.Txn.Sender
		}
		if txn.Authorizer() != correctAuthorizer {
			return fmt.Errorf("transaction %v: should have been authorized by %v but was actually authorized by %v", txn.ID(), correctAuthorizer, txn.Authorizer())
		}
	}

	spec := transactions.SpecialAddresses{
		FeeSink:     eval.block.BlockHeader.FeeSink,
		RewardsPool: eval.block.BlockHeader.RewardsPool,
	}

	applyData, err := eval.applyTransaction(txn.Txn, cow, evalParams, spec, cow.txnCounter())
	if err != nil {
		return fmt.Errorf("transaction %v: %v", txid, err)
	}

	if eval.validate && !eval.generate {
		if eval.proto.ApplyData {
			if !ad.Equal(applyData) {
				return fmt.Errorf("transaction %v: applyData mismatch: %v != %v", txid, ad, applyData)
			}
		} else {
			if !ad.Equal(transactions.ApplyData{}) {
				return fmt.Errorf("transaction %v: applyData not supported", txid)
			}
		}
	}

	*txib, err = eval.block.EncodeSignedTxn(txn, applyData)
	if err != nil {
		return err
	}

	rewardlvl := cow.rewardsLevel()
	for _, addr := range cow.modifiedAccounts() {
		if addr == spec.FeeSink || addr == spec.RewardsPool || addr == transactions.CompactCertSender {
			continue
		}

		data, err := cow.lookup(addr)
		if err != nil {
			return err
		}

		if data.IsZero() {
			continue
		}

		dataNew := data.WithUpdatedRewards(eval.proto, rewardlvl)
		effectiveMinBalance := dataNew.MinBalance(&eval.proto)
		if dataNew.MicroAlgos.Raw < effectiveMinBalance.Raw {
			return fmt.Errorf("transaction %v: account %v balance %d below min %d (%d assets)",
				txid, addr, dataNew.MicroAlgos.Raw, effectiveMinBalance.Raw, len(dataNew.Assets))
		}

		if eval.proto.MaximumMinimumBalance != 0 {
			if effectiveMinBalance.Raw > eval.proto.MaximumMinimumBalance {
				return fmt.Errorf("transaction %v: account %v would use too much space after this transaction. Minimum balance requirements would be %d (greater than max %d)", txid, addr, effectiveMinBalance.Raw, eval.proto.MaximumMinimumBalance)
			}
		}
	}

	cow.addTx(txn.Txn, txid)

	return nil
}

func (eval *BlockEvaluator) applyTransaction(tx transactions.Transaction, balances *roundCowState, evalParams *logic.EvalParams, spec transactions.SpecialAddresses, ctr uint64) (ad transactions.ApplyData, err error) {
	params := balances.ConsensusParams()

	err = balances.Move(tx.Sender, spec.FeeSink, tx.Fee, &ad.SenderRewards, nil)
	if err != nil {
		return
	}

	if (tx.RekeyTo != basics.Address{}) {
		var acct basics.AccountData
		acct, err = balances.Get(tx.Sender, false)
		if err != nil {
			return
		}
		if tx.RekeyTo == tx.Sender {
			acct.AuthAddr = basics.Address{}
		} else {
			acct.AuthAddr = tx.RekeyTo
		}

		err = balances.Put(tx.Sender, acct)
		if err != nil {
			return
		}
	}

	switch tx.Type {
	case protocol.PaymentTx:
		err = apply.Payment(tx.PaymentTxnFields, tx.Header, balances, spec, &ad)

	case protocol.KeyRegistrationTx:
		err = apply.Keyreg(tx.KeyregTxnFields, tx.Header, balances, spec, &ad)

	case protocol.AssetConfigTx:
		err = apply.AssetConfig(tx.AssetConfigTxnFields, tx.Header, balances, spec, &ad, ctr)

	case protocol.AssetTransferTx:
		err = apply.AssetTransfer(tx.AssetTransferTxnFields, tx.Header, balances, spec, &ad)

	case protocol.AssetFreezeTx:
		err = apply.AssetFreeze(tx.AssetFreezeTxnFields, tx.Header, balances, spec, &ad)

	case protocol.ApplicationCallTx:
		err = apply.ApplicationCall(tx.ApplicationCallTxnFields, tx.Header, balances, &ad, evalParams, ctr)

	case protocol.CompactCertTx:
		if eval.validate || eval.generate {
			err = balances.compactCert(tx.CertRound, tx.CertType, tx.Cert, tx.Header.FirstValid, eval.validate)
		}

	default:
		err = fmt.Errorf("Unknown transaction type %v", tx.Type)
	}

	if !params.RewardsInApplyData {
		ad.SenderRewards = basics.MicroAlgos{}
		ad.ReceiverRewards = basics.MicroAlgos{}
		ad.CloseRewards = basics.MicroAlgos{}
	}

	return
}

func (eval *BlockEvaluator) compactCertVotersAndTotal() (root crypto.Digest, total basics.MicroAlgos, err error) {
	if eval.proto.CompactCertRounds == 0 {
		return
	}

	if eval.block.Round()%basics.Round(eval.proto.CompactCertRounds) != 0 {
		return
	}

	lookback := eval.block.Round().SubSaturate(basics.Round(eval.proto.CompactCertVotersLookback))
	voters, err := eval.l.CompactCertVoters(lookback)
	if err != nil {
		return
	}

	if voters != nil {
		root = voters.Tree.Root()
		total = voters.TotalWeight
	}

	return
}

func (eval *BlockEvaluator) endOfBlock() error {
	if eval.generate {
		var err error
		eval.block.TxnRoot, err = eval.block.PaysetCommit()
		if err != nil {
			return err
		}

		if eval.proto.TxnCounter {
			eval.block.TxnCounter = eval.state.txnCounter()
		} else {
			eval.block.TxnCounter = 0
		}

		if eval.proto.CompactCertRounds > 0 {
			var basicCompactCert bookkeeping.CompactCertState
			basicCompactCert.CompactCertVoters, basicCompactCert.CompactCertVotersTotal, err = eval.compactCertVotersAndTotal()
			if err != nil {
				return err
			}

			basicCompactCert.CompactCertNextRound = eval.state.compactCertNext()

			eval.block.CompactCert = make(map[protocol.CompactCertType]bookkeeping.CompactCertState)
			eval.block.CompactCert[protocol.CompactCertBasic] = basicCompactCert
		}
	}

	return nil
}

func (eval *BlockEvaluator) finalValidation() error {
	eval.state.mods.OptimizeAllocatedMemory(eval.proto)
	if eval.validate {
		txnRoot, err := eval.block.PaysetCommit()
		if err != nil {
			return err
		}
		if txnRoot != eval.block.TxnRoot {
			return fmt.Errorf("txn root wrong: %v != %v", txnRoot, eval.block.TxnRoot)
		}

		var expectedTxnCount uint64
		if eval.proto.TxnCounter {
			expectedTxnCount = eval.state.txnCounter()
		}
		if eval.block.TxnCounter != expectedTxnCount {
			return fmt.Errorf("txn count wrong: %d != %d", eval.block.TxnCounter, expectedTxnCount)
		}

		expectedVoters, expectedVotersWeight, err := eval.compactCertVotersAndTotal()
		if err != nil {
			return err
		}
		if eval.block.CompactCert[protocol.CompactCertBasic].CompactCertVoters != expectedVoters {
			return fmt.Errorf("CompactCertVoters wrong: %v != %v", eval.block.CompactCert[protocol.CompactCertBasic].CompactCertVoters, expectedVoters)
		}
		if eval.block.CompactCert[protocol.CompactCertBasic].CompactCertVotersTotal != expectedVotersWeight {
			return fmt.Errorf("CompactCertVotersTotal wrong: %v != %v", eval.block.CompactCert[protocol.CompactCertBasic].CompactCertVotersTotal, expectedVotersWeight)
		}
		if eval.block.CompactCert[protocol.CompactCertBasic].CompactCertNextRound != eval.state.compactCertNext() {
			return fmt.Errorf("CompactCertNextRound wrong: %v != %v", eval.block.CompactCert[protocol.CompactCertBasic].CompactCertNextRound, eval.state.compactCertNext())
		}
		for ccType := range eval.block.CompactCert {
			if ccType != protocol.CompactCertBasic {
				return fmt.Errorf("CompactCertType %d unexpected", ccType)
			}
		}
	}

	return nil
}

func (eval *BlockEvaluator) GenerateBlock() (*ValidatedBlock, error) {
	if !eval.generate {
		logging.Base().Panicf("GenerateBlock() called but generate is false")
	}

	if eval.blockGenerated {
		return nil, fmt.Errorf("GenerateBlock already called on this BlockEvaluator")
	}

	err := eval.endOfBlock()
	if err != nil {
		return nil, err
	}

	err = eval.finalValidation()
	if err != nil {
		return nil, err
	}

	vb := ValidatedBlock{
		blk:   eval.block,
		delta: eval.state.deltas(),
	}
	eval.blockGenerated = true
	eval.state = makeRoundCowState(eval.state, eval.block.BlockHeader, eval.prevHeader.TimeStamp, len(eval.block.Payset))
	return &vb, nil
}

type evalTxValidator struct {
	txcache          verify.VerifiedTransactionCache
	block            bookkeeping.Block
	verificationPool execpool.BacklogPool

	ctx      context.Context
	txgroups [][]transactions.SignedTxnWithAD
	done     chan error
}

func (validator *evalTxValidator) run() {
	defer close(validator.done)
	specialAddresses := transactions.SpecialAddresses{
		FeeSink:     validator.block.BlockHeader.FeeSink,
		RewardsPool: validator.block.BlockHeader.RewardsPool,
	}

	var unverifiedTxnGroups [][]transactions.SignedTxn
	unverifiedTxnGroups = make([][]transactions.SignedTxn, 0, len(validator.txgroups))
	for _, group := range validator.txgroups {
		signedTxnGroup := make([]transactions.SignedTxn, len(group))
		for j, txn := range group {
			signedTxnGroup[j] = txn.SignedTxn
			err := txn.SignedTxn.Txn.Alive(validator.block)
			if err != nil {
				validator.done <- err
				return
			}
		}
		unverifiedTxnGroups = append(unverifiedTxnGroups, signedTxnGroup)
	}

	unverifiedTxnGroups = validator.txcache.GetUnverifiedTranscationGroups(unverifiedTxnGroups, specialAddresses, validator.block.BlockHeader.CurrentProtocol)

	err := verify.PaysetGroups(validator.ctx, unverifiedTxnGroups, validator.block.BlockHeader, validator.verificationPool, validator.txcache)
	if err != nil {
		validator.done <- err
	}
}

func eval(ctx context.Context, l ledgerForEvaluator, blk bookkeeping.Block, validate bool, txcache verify.VerifiedTransactionCache, executionPool execpool.BacklogPool) (ledgercore.StateDelta, error) {
	eval, err := startEvaluator(l, blk.BlockHeader, len(blk.Payset), validate, false)
	if err != nil {
		return ledgercore.StateDelta{}, err
	}

	validationCtx, validationCancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		validationCancel()
		wg.Wait()
	}()

	paysetgroups, err := blk.DecodePaysetGroups()
	if err != nil {
		return ledgercore.StateDelta{}, err
	}

	paysetgroupsCh := loadAccounts(ctx, l, blk.Round()-1, paysetgroups, blk.BlockHeader.FeeSink, blk.ConsensusProtocol())

	var txvalidator evalTxValidator
	if validate {
		_, ok := config.Consensus[blk.CurrentProtocol]
		if !ok {
			return ledgercore.StateDelta{}, protocol.Error(blk.CurrentProtocol)
		}
		txvalidator.txcache = txcache
		txvalidator.block = blk
		txvalidator.verificationPool = executionPool

		txvalidator.ctx = validationCtx
		txvalidator.txgroups = paysetgroups
		txvalidator.done = make(chan error, 1)
		go txvalidator.run()

	}

	base := eval.state.lookupParent.(*roundCowBase)

transactionGroupLoop:
	for {
		select {
		case txgroup, ok := <-paysetgroupsCh:
			if !ok {
				break transactionGroupLoop
			} else if txgroup.err != nil {
				return ledgercore.StateDelta{}, err
			}

			for _, br := range txgroup.balances {
				base.accounts[br.Addr] = br.AccountData
			}
			err = eval.TransactionGroup(txgroup.group)
			if err != nil {
				return ledgercore.StateDelta{}, err
			}
		case <-ctx.Done():
			return ledgercore.StateDelta{}, ctx.Err()
		case err, open := <-txvalidator.done:
			if open && err != nil {
				return ledgercore.StateDelta{}, err
			}
		}
	}

	err = eval.endOfBlock()
	if err != nil {
		return ledgercore.StateDelta{}, err
	}

	if validate {
		select {
		case <-ctx.Done():
			return ledgercore.StateDelta{}, ctx.Err()
		case err, open := <-txvalidator.done:
			if !open {
				break
			}
			if err != nil {
				return ledgercore.StateDelta{}, err
			}
		}
		err = eval.finalValidation()
		if err != nil {
			return ledgercore.StateDelta{}, err
		}
	}

	return eval.state.deltas(), nil
}

type loadedTransactionGroup struct {
	group    []transactions.SignedTxnWithAD
	balances []basics.BalanceRecord
	err      error
}

func loadAccounts(ctx context.Context, l ledgerForEvaluator, rnd basics.Round, groups [][]transactions.SignedTxnWithAD, feeSinkAddr basics.Address, consensusParams config.ConsensusParams) chan loadedTransactionGroup {
	outChan := make(chan loadedTransactionGroup, len(groups))
	go func() {
		type groupTask struct {
			balances      []basics.BalanceRecord
			balancesCount int
			done          chan error
		}
		type addrTask struct {
			address      basics.Address
			groups       []*groupTask
			groupIndices []int
		}
		defer close(outChan)

		accountTasks := make(map[basics.Address]*addrTask)
		maxAddressesPerTransaction := 7 + consensusParams.MaxAppTxnAccounts
		addressesCh := make(chan *addrTask, len(groups)*consensusParams.MaxTxGroupSize*maxAddressesPerTransaction)
		totalBalances := 0

		initAccount := func(addr basics.Address, wg *groupTask) {
			if addr.IsZero() {
				return
			}
			if task, have := accountTasks[addr]; !have {
				task := &addrTask{
					address:      addr,
					groups:       make([]*groupTask, 1, 4),
					groupIndices: make([]int, 1, 4),
				}
				task.groups[0] = wg
				task.groupIndices[0] = wg.balancesCount

				accountTasks[addr] = task
				addressesCh <- task
			} else {
				task.groups = append(task.groups, wg)
				task.groupIndices = append(task.groupIndices, wg.balancesCount)
			}
			wg.balancesCount++
			totalBalances++
		}
		if len(groups) > 0 {
			task := &addrTask{
				address: feeSinkAddr,
			}
			addressesCh <- task
			accountTasks[feeSinkAddr] = task
		}

		groupsReady := make([]*groupTask, len(groups))
		for i, group := range groups {
			task := &groupTask{}
			groupsReady[i] = task
			for _, stxn := range group {
				initAccount(stxn.Txn.Sender, task)
				initAccount(stxn.Txn.Receiver, task)
				initAccount(stxn.Txn.CloseRemainderTo, task)
				initAccount(stxn.Txn.AssetSender, task)
				initAccount(stxn.Txn.AssetReceiver, task)
				initAccount(stxn.Txn.AssetCloseTo, task)
				initAccount(stxn.Txn.FreezeAccount, task)
				for _, xa := range stxn.Txn.Accounts {
					initAccount(xa, task)
				}
			}
		}

		if len(groupsReady) > 0 {
			initAccount(feeSinkAddr, groupsReady[0])
		}
		close(addressesCh)

		allBalances := make([]basics.BalanceRecord, totalBalances)
		usedBalances := 0
		for _, gr := range groupsReady {
			gr.balances = allBalances[usedBalances : usedBalances+gr.balancesCount]
			gr.done = make(chan error, gr.balancesCount)
			usedBalances += gr.balancesCount
		}

		for i := 0; i < asyncAccountLoadingThreadCount; i++ {
			go func() {
				for {
					select {
					case task, ok := <-addressesCh:
						if !ok {
							return
						}
						acctData, _, err := l.LookupWithoutRewards(rnd, task.address)
						br := basics.BalanceRecord{
							Addr:        task.address,
							AccountData: acctData,
						}
						if err == nil {
							for i, wg := range task.groups {
								wg.balances[task.groupIndices[i]] = br
								wg.done <- nil
							}
						} else {
							for _, wg := range task.groups {
								wg.done <- err
							}
						}
					case <-ctx.Done():
						return
					}

				}
			}()
		}

		for i, wg := range groupsReady {
			for j := 0; j < wg.balancesCount; j++ {
				select {
				case err := <-wg.done:
					if err != nil {
						outChan <- loadedTransactionGroup{
							group: groups[i],
							err:   err,
						}
						return
					}
				case <-ctx.Done():
					return
				}
			}
			outChan <- loadedTransactionGroup{
				group:    groups[i],
				balances: wg.balances,
			}
		}
	}()
	return outChan
}

func (l *Ledger) Validate(ctx context.Context, blk bookkeeping.Block, executionPool execpool.BacklogPool) (*ValidatedBlock, error) {
	delta, err := eval(ctx, l, blk, true, l.verifiedTxnCache, executionPool)
	if err != nil {
		return nil, err
	}

	vb := ValidatedBlock{
		blk:   blk,
		delta: delta,
	}
	return &vb, nil
}

type ValidatedBlock struct {
	blk   bookkeeping.Block
	delta ledgercore.StateDelta
}

func (vb ValidatedBlock) Block() bookkeeping.Block {
	return vb.blk
}

func (vb ValidatedBlock) WithSeed(s committee.Seed) ValidatedBlock {
	newblock := vb.blk
	newblock.BlockHeader.Seed = s

	return ValidatedBlock{
		blk:   newblock,
		delta: vb.delta,
	}
}
