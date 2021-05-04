package network

type GossipNode interface {
	Address() (string, bool)
	Start()
}

type WebsocketNetwork struct {
}

func (wn *WebsocketNetwork) Address() (string, bool) {
	return "hi", true
}
func (wn *WebsocketNetwork) Start() {
}
