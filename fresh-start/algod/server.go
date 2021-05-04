package algod

import "fresh/data/bookkeeping"

type Server struct {
	RootPath string
	Genesis  bookkeeping.Genesis
}

func (s *Server) Start() {
}
