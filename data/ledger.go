
package data

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/algorand/go-algorand/agreement"
	"github.com/algorand/go-algorand/config"
	"github.com/algorand/go-algorand/crypto"
	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/committee"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/ledger"
	"github.com/algorand/go-algorand/ledger/ledgercore"
	"github.com/algorand/go-algorand/logging"
	"github.com/algorand/go-algorand/protocol"
)

type Ledger struct {
	*ledger.Ledger

	log logging.Logger

	lastRoundCirculation atomic.Value
	lastRoundSeed atomic.Value
}

type roundCirculationPair struct {
	round       basics.Round
	onlineMoney basics.MicroAlgos
}

type roundCirculation struct {
	elements [2]roundCirculationPair
}

type roundSeedPair struct {
	round basics.Round
	seed  committee.Seed
}

type roundSeed struct {
	elements [2]roundSeedPair
}

func makeGenesisBlock(proto protocol.ConsensusVersion, genesisBal GenesisBalances, genesisID string, genesisHash crypto.Digest) (bookkeeping.Block, error) {
	params, ok := config.Consensus[proto]
	if !ok {
		return bookkeeping.Block{}, fmt.Errorf("unsupported protocol %s", proto)
	}

	poolAddr := basics.Address(genesisBal.rewardsPool)
	incentivePoolBalanceAtGenesis := genesisBal.balances[poolAddr].MicroAlgos

	genesisRewardsState := bookkeeping.RewardsState{
		FeeSink:                   genesisBal.feeSink,
		RewardsPool:               genesisBal.rewardsPool,
		RewardsLevel:              0,
		RewardsResidue:            0,
		RewardsRecalculationRound: basics.Round(params.RewardsRateRefreshInterval),
	}

	if params.InitialRewardsRateCalculation {
		genesisRewardsState.RewardsRate = basics.SubSaturate(incentivePoolBalanceAtGenesis.Raw, params.MinBalance) / uint64(params.RewardsRateRefreshInterval)
	} else {
		genesisRewardsState.RewardsRate = incentivePoolBalanceAtGenesis.Raw / uint64(params.RewardsRateRefreshInterval)
	}

	genesisProtoState := bookkeeping.UpgradeState{
		CurrentProtocol: proto,
	}

	blk := bookkeeping.Block{
		BlockHeader: bookkeeping.BlockHeader{
			Round:        0,
			Branch:       bookkeeping.BlockHash{},
			Seed:         committee.Seed(genesisHash),
			TxnRoot:      transactions.Payset{}.CommitGenesis(),
			TimeStamp:    genesisBal.timestamp,
			GenesisID:    genesisID,
			RewardsState: genesisRewardsState,
			UpgradeState: genesisProtoState,
			UpgradeVote:  bookkeeping.UpgradeVote{},
		},
	}

	if params.SupportGenesisHash {
		blk.BlockHeader.GenesisHash = genesisHash
	}

	return blk, nil
}

func LoadLedger(
	log logging.Logger, dbFilenamePrefix string, memory bool,
	genesisProto protocol.ConsensusVersion, genesisBal GenesisBalances, genesisID string, genesisHash crypto.Digest,
	blockListeners []ledger.BlockListener, cfg config.Local,
) (*Ledger, error) {
	if genesisBal.balances == nil {
		genesisBal.balances = make(map[basics.Address]basics.AccountData)
	}
	genBlock, err := makeGenesisBlock(genesisProto, genesisBal, genesisID, genesisHash)
	if err != nil {
		return nil, err
	}

	params := config.Consensus[genesisProto]
	if params.ForceNonParticipatingFeeSink {
		sinkAddr := genesisBal.feeSink
		sinkData := genesisBal.balances[sinkAddr]
		sinkData.Status = basics.NotParticipating
		genesisBal.balances[sinkAddr] = sinkData
	}

	l := &Ledger{
		log: log,
	}
	genesisInitState := ledger.InitState{
		Block:       genBlock,
		Accounts:    genesisBal.balances,
		GenesisHash: genesisHash,
	}
	l.log.Debugf("Initializing Ledger(%s)", dbFilenamePrefix)

	ll, err := ledger.OpenLedger(log, dbFilenamePrefix, memory, genesisInitState, cfg)
	if err != nil {
		return nil, err
	}

	l.Ledger = ll
	l.RegisterBlockListeners(blockListeners)
	return l, nil
}

func (l *Ledger) AddressTxns(id basics.Address, r basics.Round) ([]transactions.SignedTxnWithAD, error) {
	blk, err := l.Block(r)
	if err != nil {
		return nil, err
	}
	spec := transactions.SpecialAddresses{
		FeeSink:     blk.FeeSink,
		RewardsPool: blk.RewardsPool,
	}

	var res []transactions.SignedTxnWithAD
	payset, err := blk.DecodePaysetFlat()
	if err != nil {
		return nil, err
	}
	for _, tx := range payset {
		if tx.Txn.MatchAddress(id, spec) {
			res = append(res, tx)
		}
	}
	return res, nil
}

func (l *Ledger) LookupTxid(txid transactions.Txid, r basics.Round) (stxn transactions.SignedTxnWithAD, found bool, err error) {
	var blk bookkeeping.Block
	blk, err = l.Block(r)
	if err != nil {
		return transactions.SignedTxnWithAD{}, false, err
	}

	payset, err := blk.DecodePaysetFlat()
	if err != nil {
		return transactions.SignedTxnWithAD{}, false, err
	}
	for _, tx := range payset {
		if tx.ID() == txid {
			return tx, true, nil
		}
	}
	return transactions.SignedTxnWithAD{}, false, nil
}

func (l *Ledger) LastRound() basics.Round {
	return l.Latest()
}

func (l *Ledger) NextRound() basics.Round {
	return l.LastRound() + 1
}

func (l *Ledger) Circulation(r basics.Round) (basics.MicroAlgos, error) {
	circulation, cached := l.lastRoundCirculation.Load().(roundCirculation)
	if cached && r != basics.Round(0) {
		for _, element := range circulation.elements {
			if element.round == r {
				return element.onlineMoney, nil
			}
		}
	}

	totals, err := l.Totals(r)
	if err != nil {
		return basics.MicroAlgos{}, err
	}

	if !cached || r > circulation.elements[1].round {
		l.lastRoundCirculation.Store(
			roundCirculation{
				elements: [2]roundCirculationPair{
					circulation.elements[1],
					{
						round:       r,
						onlineMoney: totals.Online.Money},
				},
			})
	}

	return totals.Online.Money, nil
}

func (l *Ledger) Seed(r basics.Round) (committee.Seed, error) {
	seed, cached := l.lastRoundSeed.Load().(roundSeed)
	if cached && r != basics.Round(0) {
		for _, roundSeed := range seed.elements {
			if roundSeed.round == r {
				return roundSeed.seed, nil
			}
		}
	}

	blockhdr, err := l.BlockHdr(r)
	if err != nil {
		return committee.Seed{}, err
	}

	if !cached || r > seed.elements[1].round {
		l.lastRoundSeed.Store(
			roundSeed{
				elements: [2]roundSeedPair{
					seed.elements[1],
					{
						round: r,
						seed:  blockhdr.Seed,
					},
				},
			})
	}

	return blockhdr.Seed, nil
}

func (l *Ledger) LookupDigest(r basics.Round) (crypto.Digest, error) {
	blockhdr, err := l.BlockHdr(r)
	if err != nil {
		return crypto.Digest{}, err
	}
	return crypto.Digest(blockhdr.Hash()), nil
}

func (l *Ledger) ConsensusParams(r basics.Round) (config.ConsensusParams, error) {
	blockhdr, err := l.BlockHdr(r)
	if err != nil {
		return config.ConsensusParams{}, err
	}
	return config.Consensus[blockhdr.UpgradeState.CurrentProtocol], nil
}

func (l *Ledger) ConsensusVersion(r basics.Round) (protocol.ConsensusVersion, error) {
	blockhdr, err := l.BlockHdr(r)
	if err == nil {
		return blockhdr.UpgradeState.CurrentProtocol, nil
	}
	latestCommittedRound, latestRound := l.LatestCommitted()
	if r < latestRound {
		return "", err
	}
	latestBlockhdr, err := l.BlockHdr(latestRound)
	if err == nil {
		if latestBlockhdr.NextProtocolSwitchOn == 0 {
			currentConsensusParams, _ := config.Consensus[latestBlockhdr.CurrentProtocol]
			if r <= latestBlockhdr.Round+basics.Round(currentConsensusParams.UpgradeVoteRounds) {
				return latestBlockhdr.CurrentProtocol, nil
			}
			return "", ledgercore.ErrNoEntry{Round: r, Latest: latestRound, Committed: latestCommittedRound}
		}
		if r < latestBlockhdr.NextProtocolSwitchOn {
			return latestBlockhdr.CurrentProtocol, nil
		}
		if r == latestBlockhdr.NextProtocolSwitchOn && latestBlockhdr.Round >= latestBlockhdr.NextProtocolVoteBefore {
			return latestBlockhdr.NextProtocol, nil
		}
		err = ledgercore.ErrNoEntry{Round: r, Latest: latestRound, Committed: latestCommittedRound}
	}
	return "", err
}

func (l *Ledger) EnsureValidatedBlock(vb *ledger.ValidatedBlock, c agreement.Certificate) {
	round := vb.Block().Round()

	for l.LastRound() < round {
		err := l.AddValidatedBlock(*vb, c)
		if err == nil {
			break
		}

		logfn := logging.Base().Errorf

		switch err.(type) {
		case ledgercore.BlockInLedgerError:
			logfn = logging.Base().Debugf
		}

		logfn("could not write block %d to the ledger: %v", round, err)
	}
}

func (l *Ledger) EnsureBlock(block *bookkeeping.Block, c agreement.Certificate) {
	round := block.Round()
	protocolErrorLogged := false

	for l.LastRound() < round {
		err := l.AddBlock(*block, c)
		if err == nil {
			break
		}

		switch err.(type) {
		case protocol.Error:
			if !protocolErrorLogged {
				logging.Base().Errorf("unrecoverable protocol error detected at block %d: %v", round, err)
				protocolErrorLogged = true
			}
		case ledgercore.BlockInLedgerError:
			logging.Base().Debugf("could not write block %d to the ledger: %v", round, err)
			return // this error implies that l.LastRound() >= round
		default:
			logging.Base().Errorf("could not write block %d to the ledger: %v", round, err)
		}

		time.Sleep(100 * time.Millisecond)
	}
}
