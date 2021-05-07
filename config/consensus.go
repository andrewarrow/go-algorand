
package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/algorand/go-algorand/protocol"
)

type ConsensusParams struct {
	UpgradeVoteRounds        uint64
	UpgradeThreshold         uint64
	DefaultUpgradeWaitRounds uint64
	MinUpgradeWaitRounds     uint64
	MaxUpgradeWaitRounds     uint64
	MaxVersionStringLen      int

	MaxTxnBytesPerBlock int

	MaxTxnNoteBytes int

	MaxTxnLife uint64

	ApprovedUpgrades map[protocol.ConsensusVersion]uint64

	SupportGenesisHash bool

	RequireGenesisHash bool

	DefaultKeyDilution uint64

	MinBalance uint64

	MinTxnFee uint64

	RewardUnit uint64

	RewardsRateRefreshInterval uint64

	SeedLookback        uint64 // how many blocks back we use seeds from in sortition. delta_s in the spec
	SeedRefreshInterval uint64 // how often an old block hash is mixed into the seed. delta_r in the spec

	MaxBalLookback uint64 // (current round - MaxBalLookback) is the oldest round the ledger must answer balance queries for

	NumProposers           uint64
	SoftCommitteeSize      uint64
	SoftCommitteeThreshold uint64
	CertCommitteeSize      uint64
	CertCommitteeThreshold uint64
	NextCommitteeSize      uint64 // for any non-FPR votes >= deadline step, committee sizes and thresholds are constant
	NextCommitteeThreshold uint64
	LateCommitteeSize      uint64
	LateCommitteeThreshold uint64
	RedoCommitteeSize      uint64
	RedoCommitteeThreshold uint64
	DownCommitteeSize      uint64
	DownCommitteeThreshold uint64

	AgreementFilterTimeout time.Duration
	AgreementFilterTimeoutPeriod0 time.Duration

	FastRecoveryLambda    time.Duration // time between fast recovery attempts
	FastPartitionRecovery bool          // set when fast partition recovery is enabled

	PaysetCommit PaysetCommitType

	MaxTimestampIncrement int64 // maximum time between timestamps on successive blocks

	SupportSignedTxnInBlock bool

	ForceNonParticipatingFeeSink bool

	ApplyData bool

	RewardsInApplyData bool

	CredentialDomainSeparationEnabled bool

	SupportBecomeNonParticipatingTransactions bool

	PendingResidueRewards bool

	Asset bool

	MaxAssetsPerAccount int

	MaxAssetNameBytes int

	MaxAssetUnitNameBytes int

	MaxAssetURLBytes int

	TxnCounter bool

	SupportTxGroups bool

	MaxTxGroupSize int

	SupportTransactionLeases bool
	FixTransactionLeases     bool

	LogicSigVersion uint64

	LogicSigMaxSize uint64

	LogicSigMaxCost uint64

	MaxAssetDecimals uint32

	SupportRekeying bool

	Application bool

	MaxAppArgs int

	MaxAppTotalArgLen int

	MaxAppProgramLen int

	MaxAppTxnAccounts int

	MaxAppTxnForeignApps int

	MaxAppTxnForeignAssets int

	MaxAppProgramCost int

	MaxAppKeyLen int

	MaxAppBytesValueLen int

	MaxAppsCreated int

	MaxAppsOptedIn int

	AppFlatParamsMinBalance uint64

	AppFlatOptInMinBalance uint64

	SchemaMinBalancePerEntry uint64

	SchemaUintMinBalance uint64

	SchemaBytesMinBalance uint64

	MaxLocalSchemaEntries uint64

	MaxGlobalSchemaEntries uint64

	MaximumMinimumBalance uint64

	CompactCertRounds uint64

	CompactCertTopVoters uint64

	CompactCertVotersLookback uint64

	CompactCertWeightThreshold uint32

	CompactCertSecKQ uint64

	EnableAssetCloseAmount bool

	InitialRewardsRateCalculation bool

	NoEmptyLocalDeltas bool
}

type PaysetCommitType int

const (
	PaysetCommitUnsupported PaysetCommitType = iota

	PaysetCommitFlat

	PaysetCommitMerkle
)

type ConsensusProtocols map[protocol.ConsensusVersion]ConsensusParams

var Consensus ConsensusProtocols

var MaxVoteThreshold int

var MaxEvalDeltaAccounts int

var MaxStateDeltaKeys int

var MaxLogicSigMaxSize int

var MaxTxnNoteBytes int

var MaxTxGroupSize int

var MaxAppProgramLen int

var MaxBytesKeyValueLen int

func checkSetMax(value int, curMax *int) {
	if value > *curMax {
		*curMax = value
	}
}

func checkSetAllocBounds(p ConsensusParams) {
	checkSetMax(int(p.SoftCommitteeThreshold), &MaxVoteThreshold)
	checkSetMax(int(p.CertCommitteeThreshold), &MaxVoteThreshold)
	checkSetMax(int(p.NextCommitteeThreshold), &MaxVoteThreshold)
	checkSetMax(int(p.LateCommitteeThreshold), &MaxVoteThreshold)
	checkSetMax(int(p.RedoCommitteeThreshold), &MaxVoteThreshold)
	checkSetMax(int(p.DownCommitteeThreshold), &MaxVoteThreshold)

	checkSetMax(p.MaxAppProgramLen, &MaxStateDeltaKeys)
	checkSetMax(p.MaxAppProgramLen, &MaxEvalDeltaAccounts)
	checkSetMax(p.MaxAppProgramLen, &MaxAppProgramLen)
	checkSetMax(int(p.LogicSigMaxSize), &MaxLogicSigMaxSize)
	checkSetMax(p.MaxTxnNoteBytes, &MaxTxnNoteBytes)
	checkSetMax(p.MaxTxGroupSize, &MaxTxGroupSize)
	checkSetMax(p.MaxAppKeyLen, &MaxBytesKeyValueLen)
	checkSetMax(p.MaxAppBytesValueLen, &MaxBytesKeyValueLen)
}

func SaveConfigurableConsensus(dataDirectory string, params ConsensusProtocols) error {
	consensusProtocolPath := filepath.Join(dataDirectory, ConfigurableConsensusProtocolsFilename)

	if len(params) == 0 {
		err := os.Remove(consensusProtocolPath)
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	encodedConsensusParams, err := json.Marshal(params)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(consensusProtocolPath, encodedConsensusParams, 0644)
	return err
}

func (cp ConsensusProtocols) DeepCopy() ConsensusProtocols {
	staticConsensus := make(ConsensusProtocols)
	for consensusVersion, consensusParams := range cp {
		if consensusParams.ApprovedUpgrades != nil {
			newApprovedUpgrades := make(map[protocol.ConsensusVersion]uint64)
			for ver, when := range consensusParams.ApprovedUpgrades {
				newApprovedUpgrades[ver] = when
			}
			consensusParams.ApprovedUpgrades = newApprovedUpgrades
		}
		staticConsensus[consensusVersion] = consensusParams
	}
	return staticConsensus
}

func (cp ConsensusProtocols) Merge(configurableConsensus ConsensusProtocols) ConsensusProtocols {
	staticConsensus := cp.DeepCopy()

	for consensusVersion, consensusParams := range configurableConsensus {
		if consensusParams.ApprovedUpgrades == nil {
			for cVer, cParam := range staticConsensus {
				if cVer == consensusVersion {
					delete(staticConsensus, cVer)
				} else if _, has := cParam.ApprovedUpgrades[consensusVersion]; has {
					delete(cParam.ApprovedUpgrades, consensusVersion)
				}
			}
		} else {
			staticConsensus[consensusVersion] = consensusParams
		}
	}

	return staticConsensus
}

func LoadConfigurableConsensusProtocols(dataDirectory string) error {
	newConsensus, err := PreloadConfigurableConsensusProtocols(dataDirectory)
	if err != nil {
		return err
	}
	if newConsensus != nil {
		Consensus = newConsensus
		for _, p := range Consensus {
			checkSetAllocBounds(p)
		}
	}
	return nil
}

func PreloadConfigurableConsensusProtocols(dataDirectory string) (ConsensusProtocols, error) {
	consensusProtocolPath := filepath.Join(dataDirectory, ConfigurableConsensusProtocolsFilename)
	file, err := os.Open(consensusProtocolPath)

	if err != nil {
		if os.IsNotExist(err) {
			return Consensus, nil
		}
		return nil, err
	}
	defer file.Close()

	configurableConsensus := make(ConsensusProtocols)

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&configurableConsensus)
	if err != nil {
		return nil, err
	}
	return Consensus.Merge(configurableConsensus), nil
}

func initConsensusProtocols() {

	v7 := ConsensusParams{
		UpgradeVoteRounds:        10000,
		UpgradeThreshold:         9000,
		DefaultUpgradeWaitRounds: 10000,
		MaxVersionStringLen:      64,

		MinBalance:          10000,
		MinTxnFee:           1000,
		MaxTxnLife:          1000,
		MaxTxnNoteBytes:     1024,
		MaxTxnBytesPerBlock: 1000000,
		DefaultKeyDilution:  10000,

		MaxTimestampIncrement: 25,

		RewardUnit:                 1e6,
		RewardsRateRefreshInterval: 5e5,

		ApprovedUpgrades: map[protocol.ConsensusVersion]uint64{},

		NumProposers:           30,
		SoftCommitteeSize:      2500,
		SoftCommitteeThreshold: 1870,
		CertCommitteeSize:      1000,
		CertCommitteeThreshold: 720,
		NextCommitteeSize:      10000,
		NextCommitteeThreshold: 7750,
		LateCommitteeSize:      10000,
		LateCommitteeThreshold: 7750,
		RedoCommitteeSize:      10000,
		RedoCommitteeThreshold: 7750,
		DownCommitteeSize:      10000,
		DownCommitteeThreshold: 7750,

		AgreementFilterTimeout:        4 * time.Second,
		AgreementFilterTimeoutPeriod0: 4 * time.Second,

		FastRecoveryLambda: 5 * time.Minute,

		SeedLookback:        2,
		SeedRefreshInterval: 100,

		MaxBalLookback: 320,

		MaxTxGroupSize: 1,
	}

	v7.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}
	Consensus[protocol.ConsensusV7] = v7

	v8 := v7

	v8.SeedRefreshInterval = 80
	v8.NumProposers = 9
	v8.SoftCommitteeSize = 2990
	v8.SoftCommitteeThreshold = 2267
	v8.CertCommitteeSize = 1500
	v8.CertCommitteeThreshold = 1112
	v8.NextCommitteeSize = 5000
	v8.NextCommitteeThreshold = 3838
	v8.LateCommitteeSize = 5000
	v8.LateCommitteeThreshold = 3838
	v8.RedoCommitteeSize = 5000
	v8.RedoCommitteeThreshold = 3838
	v8.DownCommitteeSize = 5000
	v8.DownCommitteeThreshold = 3838

	v8.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}
	Consensus[protocol.ConsensusV8] = v8

	v7.ApprovedUpgrades[protocol.ConsensusV8] = 0

	v9 := v8
	v9.MinBalance = 100000
	v9.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}
	Consensus[protocol.ConsensusV9] = v9

	v8.ApprovedUpgrades[protocol.ConsensusV9] = 0

	v10 := v9
	v10.FastPartitionRecovery = true
	v10.NumProposers = 20
	v10.LateCommitteeSize = 500
	v10.LateCommitteeThreshold = 320
	v10.RedoCommitteeSize = 2400
	v10.RedoCommitteeThreshold = 1768
	v10.DownCommitteeSize = 6000
	v10.DownCommitteeThreshold = 4560
	v10.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}
	Consensus[protocol.ConsensusV10] = v10

	v9.ApprovedUpgrades[protocol.ConsensusV10] = 0

	v11 := v10
	v11.SupportSignedTxnInBlock = true
	v11.PaysetCommit = PaysetCommitFlat
	v11.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}
	Consensus[protocol.ConsensusV11] = v11

	v10.ApprovedUpgrades[protocol.ConsensusV11] = 0

	v12 := v11
	v12.MaxVersionStringLen = 128
	v12.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}
	Consensus[protocol.ConsensusV12] = v12

	v11.ApprovedUpgrades[protocol.ConsensusV12] = 0

	v13 := v12
	v13.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}
	Consensus[protocol.ConsensusV13] = v13

	v12.ApprovedUpgrades[protocol.ConsensusV13] = 0

	v14 := v13
	v14.ApplyData = true
	v14.SupportGenesisHash = true
	v14.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}
	Consensus[protocol.ConsensusV14] = v14

	v13.ApprovedUpgrades[protocol.ConsensusV14] = 0

	v15 := v14
	v15.RewardsInApplyData = true
	v15.ForceNonParticipatingFeeSink = true
	v15.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}
	Consensus[protocol.ConsensusV15] = v15

	v14.ApprovedUpgrades[protocol.ConsensusV15] = 0

	v16 := v15
	v16.CredentialDomainSeparationEnabled = true
	v16.RequireGenesisHash = true
	v16.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}
	Consensus[protocol.ConsensusV16] = v16

	v15.ApprovedUpgrades[protocol.ConsensusV16] = 0

	v17 := v16
	v17.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}
	Consensus[protocol.ConsensusV17] = v17

	v16.ApprovedUpgrades[protocol.ConsensusV17] = 0

	v18 := v17
	v18.PendingResidueRewards = true
	v18.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}
	v18.TxnCounter = true
	v18.Asset = true
	v18.LogicSigVersion = 1
	v18.LogicSigMaxSize = 1000
	v18.LogicSigMaxCost = 20000
	v18.MaxAssetsPerAccount = 1000
	v18.SupportTxGroups = true
	v18.MaxTxGroupSize = 16
	v18.SupportTransactionLeases = true
	v18.SupportBecomeNonParticipatingTransactions = true
	v18.MaxAssetNameBytes = 32
	v18.MaxAssetUnitNameBytes = 8
	v18.MaxAssetURLBytes = 32
	Consensus[protocol.ConsensusV18] = v18

	v19 := v18
	v19.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}

	Consensus[protocol.ConsensusV19] = v19

	v18.ApprovedUpgrades[protocol.ConsensusV19] = 0
	v17.ApprovedUpgrades[protocol.ConsensusV19] = 0

	v20 := v19
	v20.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}
	v20.MaxAssetDecimals = 19
	v20.DefaultUpgradeWaitRounds = 140000
	Consensus[protocol.ConsensusV20] = v20

	v19.ApprovedUpgrades[protocol.ConsensusV20] = 0

	v21 := v20
	v21.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}
	Consensus[protocol.ConsensusV21] = v21
	v20.ApprovedUpgrades[protocol.ConsensusV21] = 0

	v22 := v21
	v22.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}
	v22.MinUpgradeWaitRounds = 10000
	v22.MaxUpgradeWaitRounds = 150000
	Consensus[protocol.ConsensusV22] = v22

	v23 := v22
	v23.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}
	v23.FixTransactionLeases = true
	Consensus[protocol.ConsensusV23] = v23
	v22.ApprovedUpgrades[protocol.ConsensusV23] = 10000
	v21.ApprovedUpgrades[protocol.ConsensusV23] = 0

	v24 := v23
	v24.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}
	v24.LogicSigVersion = 2

	v24.Application = true

	v24.SupportRekeying = true

	v24.MaximumMinimumBalance = 100100000

	v24.MaxAppArgs = 16
	v24.MaxAppTotalArgLen = 2048
	v24.MaxAppProgramLen = 1024
	v24.MaxAppKeyLen = 64
	v24.MaxAppBytesValueLen = 64

	v24.AppFlatParamsMinBalance = 100000
	v24.AppFlatOptInMinBalance = 100000

	v24.MaxAppTxnAccounts = 4

	v24.MaxAppTxnForeignApps = 2

	v24.MaxAppTxnForeignAssets = 2

	v24.SchemaMinBalancePerEntry = 25000

	v24.SchemaUintMinBalance = 3500

	v24.SchemaBytesMinBalance = 25000

	v24.MaxLocalSchemaEntries = 16

	v24.MaxGlobalSchemaEntries = 64

	v24.MaxAppProgramCost = 700

	v24.MaxAppsCreated = 10

	v24.MaxAppsOptedIn = 10
	Consensus[protocol.ConsensusV24] = v24

	v23.ApprovedUpgrades[protocol.ConsensusV24] = 140000

	v25 := v24
	v25.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}

	v25.EnableAssetCloseAmount = true
	Consensus[protocol.ConsensusV25] = v25

	v26 := v25
	v26.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}

	v26.InitialRewardsRateCalculation = true

	v26.PaysetCommit = PaysetCommitMerkle

	v26.LogicSigVersion = 3

	Consensus[protocol.ConsensusV26] = v26

	v25.ApprovedUpgrades[protocol.ConsensusV26] = 140000
	v24.ApprovedUpgrades[protocol.ConsensusV26] = 140000

	v27 := v26
	v27.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}

	v27.NoEmptyLocalDeltas = true

	Consensus[protocol.ConsensusV27] = v27

	v26.ApprovedUpgrades[protocol.ConsensusV27] = 60000

	vFuture := v27
	vFuture.ApprovedUpgrades = map[protocol.ConsensusVersion]uint64{}

	vFuture.AgreementFilterTimeoutPeriod0 = 4 * time.Second

	vFuture.CompactCertRounds = 128
	vFuture.CompactCertTopVoters = 1024 * 1024
	vFuture.CompactCertVotersLookback = 16
	vFuture.CompactCertWeightThreshold = (1 << 32) * 30 / 100
	vFuture.CompactCertSecKQ = 128

	vFuture.InitialRewardsRateCalculation = true
	vFuture.PaysetCommit = PaysetCommitMerkle

	Consensus[protocol.ConsensusFuture] = vFuture
}

type Global struct {
	SmallLambda time.Duration // min amount of time to wait for leader's credential (i.e., time to propagate one credential)
	BigLambda   time.Duration // max amount of time to wait for leader's proposal (i.e., time to propagate one block)
}

var Protocol = Global{
	SmallLambda: 2000 * time.Millisecond,
	BigLambda:   15000 * time.Millisecond,
}

func init() {
	Consensus = make(ConsensusProtocols)

	initConsensusProtocols()

	for _, p := range Consensus {
		checkSetAllocBounds(p)
	}
}
