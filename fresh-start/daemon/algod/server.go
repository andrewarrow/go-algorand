package algod

import (
	"fresh/data/bookkeeping"
	"fresh/node"
)

type Server struct {
	RootPath             string
	Genesis              bookkeeping.Genesis
	pidFile              string
	netFile              string
	netListenFile        string
	log                  string //logging.Logger
	node                 *node.AlgorandFullNode
	metricCollector      string //*metrics.MetricService
	metricServiceStarted bool
	stopping             chan struct{}
}

func (s *Server) Initialize() {
	s.node = &node.AlgorandFullNode{}
}

func (s *Server) Start() {
	s.node.Start()
}
