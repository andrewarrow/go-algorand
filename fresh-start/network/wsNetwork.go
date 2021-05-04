package network

import (
	"fmt"
	"time"
)

type GossipNode interface {
	Address() (string, bool)
	Start()
}

type WebsocketNetwork struct {
}

func (wn *WebsocketNetwork) Address() (string, bool) {
	return "hi", true
}

func (wn *WebsocketNetwork) meshThread() {
	for {
		fmt.Println("mesh")
		time.Sleep(time.Second)
	}
}

func (wn *WebsocketNetwork) Start() {
	go wn.meshThread()
}
