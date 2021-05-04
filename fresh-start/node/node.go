package node

import "fresh/network"

type AlgorandFullNode struct {
	//  ledger *data.Ledger
	net network.GossipNode
	//   transactionPool *pools.TransactionPool
	//   txHandler       *data.TxHandler

	//  accountManager  *data.AccountManager

	genesisID string
}

func (node *AlgorandFullNode) Start() {
	node.net = &network.WebsocketNetwork{
		//log:       log,
		//config:    config,
		//phonebook: phonebook,
		//GenesisID: genesisID,
		//NetworkID: networkID,
	}
	node.net.Start()
}
