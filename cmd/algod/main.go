// Copyright (C) 2019-2021 Algorand, Inc.
// This file is part of go-algorand
//
// go-algorand is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// go-algorand is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with go-algorand.  If not, see <https://www.gnu.org/licenses/>.

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/algorand/go-algorand/config"
	"github.com/algorand/go-algorand/data/bookkeeping"
	/*
		"github.com/algorand/go-deadlock"
		"github.com/gofrs/flock"

		"github.com/algorand/go-algorand/config"
		"github.com/algorand/go-algorand/crypto"
		"github.com/algorand/go-algorand/daemon/algod"
		"github.com/algorand/go-algorand/data/bookkeeping"
		"github.com/algorand/go-algorand/logging"
		"github.com/algorand/go-algorand/logging/telemetryspec"
		"github.com/algorand/go-algorand/network"
		"github.com/algorand/go-algorand/protocol"
		toolsnet "github.com/algorand/go-algorand/tools/network"
		"github.com/algorand/go-algorand/util/metrics"
		"github.com/algorand/go-algorand/util/tokens"
	*/)

var dataDirectory = flag.String("d", "", "Root Algorand daemon data path")
var genesisFile = flag.String("g", "", "Genesis configuration file")
var genesisPrint = flag.Bool("G", false, "Print genesis ID")
var versionCheck = flag.Bool("v", false, "Display and write current build version and exit")
var branchCheck = flag.Bool("b", false, "Display the git branch behind the build")
var channelCheck = flag.Bool("c", false, "Display and release channel behind the build")
var initAndExit = flag.Bool("x", false, "Initialize the ledger and exit")
var peerOverride = flag.String("p", "", "Override phonebook with peer ip:port (or semicolon separated list: ip:port;ip:port;ip:port...)")
var listenIP = flag.String("l", "", "Override config.EndpointAddress (REST listening address) with ip:port")
var sessionGUID = flag.String("s", "", "Telemetry Session GUID to use")
var telemetryOverride = flag.String("t", "", `Override telemetry setting if supported (Use "true", "false", "0" or "1"`)
var seed = flag.String("seed", "", "input to math/rand.Seed()")

func main() {
	flag.Parse()

	dataDir := resolveDataDir()
	absolutePath, _ := filepath.Abs(dataDir)
	config.UpdateVersionDataDir(absolutePath)

	rand.Seed(time.Now().UnixNano())
	//version := config.GetCurrentVersion()

	genesisText, _ := ioutil.ReadFile("genesis.json")
	var genesis bookkeeping.Genesis
	//protocol.DecodeJSON(genesisText, &genesis)
	fmt.Printf("%+v", genesis)

}

func resolveDataDir() string {
	// Figure out what data directory to tell algod to use.
	// If not specified on cmdline with '-d', look for default in environment.
	var dir string
	if dataDirectory == nil || *dataDirectory == "" {
		dir = os.Getenv("ALGORAND_DATA")
	} else {
		dir = *dataDirectory
	}
	return dir
}
