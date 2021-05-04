package bookkeeping

import "fresh/protocol"

type Genesis struct {
	SchemaID    string
	Network     string //protocol.NetworkID
	Proto       string //protocol.ConsensusVersion
	Allocation  []GenesisAllocation
	RewardsPool string
	FeeSink     string
	Timestamp   int64
	Comment     string
}

type GenesisAllocation struct {
	Address string
	Comment string
	//State   basics.AccountData
}

func LoadGenesisFromFile(genesisFile string) (Genesis, error) {
	g := Genesis{}
	return g, nil
}
func (genesis Genesis) ID() string {
	return "hi"
}
func (genesis Genesis) ToBeHashed() (protocol.HashID, []byte) {
	var p protocol.HashID
	return p, []byte{}
}
