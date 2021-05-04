package bookkeeping

import "fresh/protocol"

type Genesis struct {
	Thing string
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
