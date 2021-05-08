
package libgoal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	algodclient "github.com/algorand/go-algorand/daemon/algod/api/client"
	v2 "github.com/algorand/go-algorand/daemon/algod/api/server/v2"
	generatedV2 "github.com/algorand/go-algorand/daemon/algod/api/server/v2/generated"
	kmdclient "github.com/algorand/go-algorand/daemon/kmd/client"
	"github.com/algorand/go-algorand/rpcs"

	"github.com/algorand/go-algorand/config"
	"github.com/algorand/go-algorand/crypto"
	"github.com/algorand/go-algorand/daemon/algod/api/spec/common"
	v1 "github.com/algorand/go-algorand/daemon/algod/api/spec/v1"
	"github.com/algorand/go-algorand/daemon/kmd/lib/kmdapi"
	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/nodecontrol"
	"github.com/algorand/go-algorand/protocol"
	"github.com/algorand/go-algorand/util"
)

const defaultKMDTimeoutSecs = 60

const DefaultKMDDataDir = nodecontrol.DefaultKMDDataDir

type Client struct {
	nc                   nodecontrol.NodeController
	kmdStartArgs         nodecontrol.KMDStartArgs
	dataDir              string
	cacheDir             string
	consensus            config.ConsensusProtocols
	algodVersionAffinity algodclient.APIVersion
	kmdVersionAffinity   kmdclient.APIVersion
}

type ClientConfig struct {
	AlgodDataDir string

	KMDDataDir string

	CacheDir string

	BinDir string
}

type ClientType int

const (
	DynamicClient ClientType = iota
	KmdClient
	AlgodClient
	FullClient
)

func MakeClientWithBinDir(binDir, dataDir, cacheDir string, clientType ClientType) (c Client, err error) {
	config := ClientConfig{
		BinDir:       binDir,
		AlgodDataDir: dataDir,
		CacheDir:     cacheDir,
	}
	err = c.init(config, clientType)
	return
}

func MakeClient(dataDir, cacheDir string, clientType ClientType) (c Client, err error) {
	binDir, err := util.ExeDir()
	if err != nil {
		return
	}
	config := ClientConfig{
		BinDir:       binDir,
		AlgodDataDir: dataDir,
		CacheDir:     cacheDir,
	}
	err = c.init(config, clientType)
	return
}

func MakeClientFromConfig(config ClientConfig, clientType ClientType) (c Client, err error) {
	if config.BinDir == "" {
		config.BinDir, err = util.ExeDir()
		if err != nil {
			return
		}
	}
	err = c.init(config, clientType)
	return
}

func (c *Client) init(config ClientConfig, clientType ClientType) error {
	dataDir, err := getDataDir(config.AlgodDataDir)
	if err != nil {
		return err
	}
	c.dataDir = dataDir
	c.cacheDir = config.CacheDir
	c.algodVersionAffinity = algodclient.APIVersionV1
	c.kmdVersionAffinity = kmdclient.APIVersionV1

	nc, err := getNodeController(config.BinDir, config.AlgodDataDir)
	if err != nil {
		return err
	}
	if config.KMDDataDir != "" {
		nc.SetKMDDataDir(config.KMDDataDir)
	} else {
		algodKmdPath, _ := filepath.Abs(filepath.Join(dataDir, DefaultKMDDataDir))
		nc.SetKMDDataDir(algodKmdPath)
	}
	c.nc = nc

	c.kmdStartArgs = nodecontrol.KMDStartArgs{
		TimeoutSecs: defaultKMDTimeoutSecs,
	}

	if clientType == KmdClient || clientType == FullClient {
		_, err = c.ensureKmdClient()
		if err != nil {
			return err
		}
	}

	if clientType == AlgodClient || clientType == FullClient {
		_, err = c.ensureAlgodClient()
		if err != nil {
			return err
		}
	}

	c.consensus, err = nc.GetConsensus()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) ensureKmdClient() (*kmdclient.KMDClient, error) {
	kmd, err := c.getKMDClient()
	if err != nil {
		return nil, err
	}
	return &kmd, nil
}

func (c *Client) ensureAlgodClient() (*algodclient.RestClient, error) {
	algod, err := c.getAlgodClient()
	if err != nil {
		return nil, err
	}
	algod.SetAPIVersionAffinity(c.algodVersionAffinity)
	return &algod, err
}

func (c *Client) DataDir() string {
	return c.dataDir
}

func getDataDir(dataDir string) (string, error) {

	dir := dataDir
	if dir == "" {
		dir = os.Getenv("ALGORAND_DATA")
	}
	if dir == "" {
		fmt.Println(errorNoDataDirectory.Error())
		return "", errorNoDataDirectory

	}
	return dir, nil
}

func getNodeController(binDir, dataDir string) (nc nodecontrol.NodeController, err error) {
	dataDir, err = getDataDir(dataDir)
	if err != nil {
		return nodecontrol.NodeController{}, nil
	}

	return nodecontrol.MakeNodeController(binDir, dataDir), nil
}

func (c *Client) SetKMDStartArgs(args nodecontrol.KMDStartArgs) {
	c.kmdStartArgs = args
}

func (c *Client) getKMDClient() (kmdclient.KMDClient, error) {
	_, err := c.nc.StartKMD(c.kmdStartArgs)
	if err != nil {
		return kmdclient.KMDClient{}, err
	}

	kmdClient, err := c.nc.KMDClient()
	if err != nil {
		return kmdclient.KMDClient{}, err
	}
	return kmdClient, nil
}

func (c *Client) getAlgodClient() (algodclient.RestClient, error) {
	algodClient, err := c.nc.AlgodClient()
	if err != nil {
		return algodclient.RestClient{}, err
	}
	return algodClient, nil
}

func (c *Client) ensureGenesisID() (string, error) {
	genesis, err := c.nc.GetGenesis()
	if err != nil {
		return "", err
	}
	return genesis.ID(), nil
}

func (c *Client) GenesisID() (string, error) {
	response, err := c.ensureGenesisID()

	if err != nil {
		return "", err
	}
	return response, nil
}

func (c *Client) FullStop() error {
	return c.nc.FullStop()
}

func (c *Client) checkHandleValidMaybeRenew(walletHandle []byte) bool {
	if len(walletHandle) == 0 {
		return false
	}
	kmd, err := c.ensureKmdClient()
	if err != nil {
		return false
	}
	_, err = kmd.RenewWalletHandle(walletHandle)
	return err == nil
}

func (c *Client) ListAddresses(walletHandle []byte) ([]string, error) {
	las, err := c.ListAddressesWithInfo(walletHandle)
	if err != nil {
		return nil, err
	}

	var addrs []string
	for _, la := range las {
		addrs = append(addrs, la.Addr)
	}

	return addrs, nil
}

func (c *Client) ListAddressesWithInfo(walletHandle []byte) ([]ListedAddress, error) {
	kmd, err := c.ensureKmdClient()
	if err != nil {
		return nil, err
	}
	response, err := kmd.ListKeys(walletHandle)
	if err != nil {
		return nil, err
	}
	response2, err := kmd.ListMultisigAddrs(walletHandle)
	if err != nil {
		return nil, err
	}

	var addresses []ListedAddress
	for _, addr := range response.Addresses {
		addresses = append(addresses, ListedAddress{
			Addr:     addr,
			Multisig: false,
		})
	}

	for _, addr := range response2.Addresses {
		addresses = append(addresses, ListedAddress{
			Addr:     addr,
			Multisig: true,
		})
	}

	return addresses, nil
}

type ListedAddress struct {
	Addr     string
	Multisig bool
}

func (c *Client) DeleteAccount(walletHandle []byte, walletPassword []byte, addr string) error {
	kmd, err := c.ensureKmdClient()
	if err != nil {
		return err
	}

	_, err = kmd.DeleteKey(walletHandle, walletPassword, addr)
	return err
}

func (c *Client) GenerateAddress(walletHandle []byte) (string, error) {
	kmd, err := c.ensureKmdClient()
	if err != nil {
		return "", err
	}
	resp, err := kmd.GenerateKey(walletHandle)
	if err != nil {
		return "", err
	}

	return resp.Address, nil
}

func (c *Client) CreateMultisigAccount(walletHandle []byte, threshold uint8, addrs []string) (string, error) {
	pks := make([]crypto.PublicKey, len(addrs))
	for i, addrStr := range addrs {
		addr, err := basics.UnmarshalChecksumAddress(addrStr)
		if err != nil {
			return "", err
		}
		pks[i] = crypto.PublicKey(addr)
	}
	kmd, err := c.ensureKmdClient()
	if err != nil {
		return "", err
	}
	resp, err := kmd.ImportMultisigAddr(walletHandle, 1, threshold, pks)
	if err != nil {
		return "", err
	}

	return resp.Address, nil
}

func (c *Client) DeleteMultisigAccount(walletHandle []byte, walletPassword []byte, addr string) error {
	kmd, err := c.ensureKmdClient()
	if err != nil {
		return err
	}

	_, err = kmd.DeleteMultisigAddr(walletHandle, walletPassword, addr)
	return err
}

func (c *Client) LookupMultisigAccount(walletHandle []byte, multisigAddr string) (info MultisigInfo, err error) {
	kmd, err := c.ensureKmdClient()
	if err != nil {
		return
	}

	resp, err := kmd.ExportMultisigAddr(walletHandle, multisigAddr)
	if err != nil {
		return
	}

	var pks []string
	for _, pk := range resp.PKs {
		addr := basics.Address(pk).String()
		pks = append(pks, addr)
	}

	info.Version = resp.Version
	info.Threshold = resp.Threshold
	info.PKs = pks
	return
}

type MultisigInfo struct {
	Version   uint8
	Threshold uint8
	PKs       []string
}

func (c *Client) SendPaymentFromWallet(walletHandle, pw []byte, from, to string, fee, amount uint64, note []byte, closeTo string, firstValid, lastValid basics.Round) (transactions.Transaction, error) {
	return c.SendPaymentFromWalletWithLease(walletHandle, pw, from, to, fee, amount, note, closeTo, [32]byte{}, firstValid, lastValid)
}

func (c *Client) SendPaymentFromWalletWithLease(walletHandle, pw []byte, from, to string, fee, amount uint64, note []byte, closeTo string, lease [32]byte, firstValid, lastValid basics.Round) (transactions.Transaction, error) {
	tx, err := c.ConstructPayment(from, to, fee, amount, note, closeTo, lease, firstValid, lastValid)
	if err != nil {
		return transactions.Transaction{}, err
	}

	return c.signAndBroadcastTransactionWithWallet(walletHandle, pw, tx)
}

func (c *Client) signAndBroadcastTransactionWithWallet(walletHandle, pw []byte, tx transactions.Transaction) (transactions.Transaction, error) {
	kmd, err := c.ensureKmdClient()
	if err != nil {
		return transactions.Transaction{}, err
	}
	resp0, err := kmd.SignTransaction(walletHandle, pw, crypto.PublicKey{}, tx)
	if err != nil {
		return transactions.Transaction{}, err
	}

	var stx transactions.SignedTxn
	err = protocol.Decode(resp0.SignedTransaction, &stx)
	if err != nil {
		return transactions.Transaction{}, err
	}

	algod, err := c.ensureAlgodClient()
	if err != nil {
		return transactions.Transaction{}, err
	}

	_, err = algod.SendRawTransaction(stx)
	if err != nil {
		return transactions.Transaction{}, err
	}
	return tx, nil
}

func (c *Client) ComputeValidityRounds(firstValid, lastValid, validRounds uint64) (uint64, uint64, error) {
	params, err := c.SuggestedParams()
	if err != nil {
		return 0, 0, err
	}
	cparams, ok := c.consensus[protocol.ConsensusVersion(params.ConsensusVersion)]
	if !ok {
		return 0, 0, fmt.Errorf("cannot construct transaction: unknown consensus protocol %s", params.ConsensusVersion)
	}

	return computeValidityRounds(firstValid, lastValid, validRounds, params.LastRound, cparams.MaxTxnLife)
}

func computeValidityRounds(firstValid, lastValid, validRounds, lastRound, maxTxnLife uint64) (uint64, uint64, error) {
	if validRounds != 0 && lastValid != 0 {
		return 0, 0, fmt.Errorf("cannot construct transaction: ambiguous input: lastValid = %d, validRounds = %d", lastValid, validRounds)
	}

	if firstValid == 0 {
		firstValid = lastRound + 1
	}

	if validRounds != 0 {
		if validRounds > maxTxnLife+1 {
			return 0, 0, fmt.Errorf("cannot construct transaction: txn validity period %d is greater than protocol max txn lifetime %d", validRounds-1, maxTxnLife)
		}
		lastValid = firstValid + validRounds - 1
	} else if lastValid == 0 {
		lastValid = firstValid + maxTxnLife
	}

	if firstValid > lastValid {
		return 0, 0, fmt.Errorf("cannot construct transaction: txn would first be valid on round %d which is after last valid round %d", firstValid, lastValid)
	} else if lastValid-firstValid > maxTxnLife {
		return 0, 0, fmt.Errorf("cannot construct transaction: txn validity period ( %d to %d ) is greater than protocol max txn lifetime %d", firstValid, lastValid, maxTxnLife)
	}

	return firstValid, lastValid, nil
}

func (c *Client) ConstructPayment(from, to string, fee, amount uint64, note []byte, closeTo string, lease [32]byte, firstValid, lastValid basics.Round) (transactions.Transaction, error) {
	fromAddr, err := basics.UnmarshalChecksumAddress(from)
	if err != nil {
		return transactions.Transaction{}, err
	}

	var toAddr basics.Address
	if to != "" {
		toAddr, err = basics.UnmarshalChecksumAddress(to)
		if err != nil {
			return transactions.Transaction{}, err
		}
	}

	params, err := c.SuggestedParams()
	if err != nil {
		return transactions.Transaction{}, err
	}

	cp, ok := c.consensus[protocol.ConsensusVersion(params.ConsensusVersion)]
	if !ok {
		return transactions.Transaction{}, fmt.Errorf("ConstructPayment: unknown consensus protocol %s", params.ConsensusVersion)
	}
	fv, lv, err := computeValidityRounds(uint64(firstValid), uint64(lastValid), 0, params.LastRound, cp.MaxTxnLife)
	if err != nil {
		return transactions.Transaction{}, err
	}

	tx := transactions.Transaction{
		Type: protocol.PaymentTx,
		Header: transactions.Header{
			Sender:     fromAddr,
			Fee:        basics.MicroAlgos{Raw: fee},
			FirstValid: basics.Round(fv),
			LastValid:  basics.Round(lv),
			Lease:      lease,
			Note:       note,
		},
		PaymentTxnFields: transactions.PaymentTxnFields{
			Receiver: toAddr,
			Amount:   basics.MicroAlgos{Raw: amount},
		},
	}

	if closeTo != "" {
		closeToAddr, err := basics.UnmarshalChecksumAddress(closeTo)
		if err != nil {
			return transactions.Transaction{}, err
		}

		tx.PaymentTxnFields.CloseRemainderTo = closeToAddr
	}

	tx.Header.GenesisID = params.GenesisID

	if cp.SupportGenesisHash {
		copy(tx.Header.GenesisHash[:], params.GenesisHash)
	}

	if fee == 0 {
		tx.Fee = basics.MulAIntSaturate(basics.MicroAlgos{Raw: params.Fee}, tx.EstimateEncodedSize())
	}
	if tx.Fee.Raw < cp.MinTxnFee {
		tx.Fee.Raw = cp.MinTxnFee
	}

	return tx, nil
}

/* Algod Wrappers */

func (c *Client) Status() (resp generatedV2.NodeStatusResponse, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		resp, err = algod.Status()
	}
	return
}

func (c *Client) AccountInformation(account string) (resp v1.Account, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		resp, err = algod.AccountInformation(account)
	}
	return
}

func (c *Client) AccountInformationV2(account string) (resp generatedV2.Account, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		resp, err = algod.AccountInformationV2(account)
	}
	return
}

func (c *Client) AccountData(account string) (accountData basics.AccountData, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		var resp []byte
		resp, err = algod.RawAccountInformationV2(account)
		if err == nil {
			err = protocol.Decode(resp, &accountData)
		}
	}
	return
}

func (c *Client) AssetInformation(index uint64) (resp v1.AssetParams, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		resp, err = algod.AssetInformation(index)
	}
	return
}

func (c *Client) AssetInformationV2(index uint64) (resp generatedV2.Asset, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		resp, err = algod.AssetInformationV2(index)
	}
	return
}

func (c *Client) ApplicationInformation(index uint64) (resp generatedV2.Application, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		resp, err = algod.ApplicationInformation(index)
	}
	return
}

func (c *Client) TransactionInformation(addr, txid string) (resp v1.Transaction, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		resp, err = algod.TransactionInformation(addr, txid)
	}
	return
}

func (c *Client) PendingTransactionInformation(txid string) (resp v1.Transaction, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		resp, err = algod.PendingTransactionInformation(txid)
	}
	return
}

func (c *Client) Block(round uint64) (resp v1.Block, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		resp, err = algod.Block(round)
	}
	return
}

func (c *Client) RawBlock(round uint64) (resp v1.RawBlock, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		resp, err = algod.RawBlock(round)
	}
	return
}

func (c *Client) BookkeepingBlock(round uint64) (block bookkeeping.Block, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		var resp []byte
		resp, err = algod.RawBlock(round)
		if err == nil {
			var b rpcs.EncodedBlockCert
			err = protocol.DecodeReflect(resp, &b)
			if err != nil {
				return
			}
			block = b.Block
		}
	}
	return
}

func (c *Client) HealthCheck() error {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		err = algod.HealthCheck()
	}
	return err
}

func (c *Client) WaitForRound(round uint64) (resp generatedV2.NodeStatusResponse, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		resp, err = algod.StatusAfterBlock(round)
	}
	return
}

func (c *Client) GetBalance(address string) (uint64, error) {
	resp, err := c.AccountInformation(address)
	if err != nil {
		return 0, err
	}
	return resp.Amount, nil
}

func (c Client) AlgodVersions() (resp common.Version, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		resp, err = algod.Versions()
	}
	return
}

func (c Client) LedgerSupply() (resp v1.Supply, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		resp, err = algod.LedgerSupply()
	}
	return
}

func (c Client) CurrentRound() (lastRound uint64, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		resp, err := algod.Status()
		if err == nil {
			lastRound = resp.LastRound
		}
	}
	return
}

func (c *Client) SuggestedFee() (fee uint64, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		resp, err := algod.SuggestedFee()
		if err == nil {
			fee = resp.Fee
		}
	}
	return
}

func (c *Client) SuggestedParams() (params v1.TransactionParams, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		params, err = algod.SuggestedParams()
	}
	return
}

func (c *Client) GetPendingTransactions(maxTxns uint64) (resp v1.PendingTransactions, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		resp, err = algod.GetPendingTransactions(maxTxns)
	}
	return
}

func (c *Client) ExportKey(walletHandle []byte, password, account string) (resp kmdapi.APIV1POSTKeyExportResponse, err error) {
	kmd, err := c.ensureKmdClient()
	if err != nil {
		return
	}

	req := kmdapi.APIV1POSTKeyExportRequest{
		WalletHandleToken: string(walletHandle),
		Address:           account,
		WalletPassword:    password,
	}
	resp = kmdapi.APIV1POSTKeyExportResponse{}
	err = kmd.DoV1Request(req, &resp)
	return resp, err
}

func (c *Client) ConsensusParams(round uint64) (consensus config.ConsensusParams, err error) {
	block, err := c.Block(round)
	if err != nil {
		return
	}

	params, ok := c.consensus[protocol.ConsensusVersion(block.CurrentProtocol)]
	if !ok {
		err = fmt.Errorf("ConsensusParams: unknown consensus protocol %s", block.CurrentProtocol)
		return
	}

	return params, nil
}

func (c *Client) SetAPIVersionAffinity(algodVersionAffinity algodclient.APIVersion, kmdVersionAffinity kmdclient.APIVersion) {
	c.algodVersionAffinity = algodVersionAffinity
	c.kmdVersionAffinity = kmdVersionAffinity
}

func (c *Client) AbortCatchup() error {
	algod, err := c.ensureAlgodClient()
	if err != nil {
		return err
	}
	algod.SetAPIVersionAffinity(algodclient.APIVersionV2)
	resp, err := algod.Status()
	if err != nil {
		return err
	}
	if resp.Catchpoint == nil || (*resp.Catchpoint) == "" {
		return nil
	}
	_, err = algod.AbortCatchup(*resp.Catchpoint)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Catchup(catchpointLabel string) error {
	algod, err := c.ensureAlgodClient()
	if err != nil {
		return err
	}
	_, err = algod.Catchup(catchpointLabel)
	if err != nil {
		return err
	}
	return nil
}

const defaultAppIdx = 1380011588

func MakeDryrunStateBytes(client Client, txnOrStxn interface{}, other []transactions.SignedTxn, proto string, format string) (result []byte, err error) {
	switch format {
	case "json":
		var gdr generatedV2.DryrunRequest
		gdr, err = MakeDryrunStateGenerated(client, txnOrStxn, other, proto)
		if err == nil {
			result = protocol.EncodeJSON(&gdr)
		}
		return
	case "msgp":
		var dr v2.DryrunRequest
		dr, err = MakeDryrunState(client, txnOrStxn, other, proto)
		if err == nil {
			result = protocol.EncodeReflect(&dr)
		}
		return
	default:
		return nil, fmt.Errorf("format %s not supported", format)
	}
}

func MakeDryrunState(client Client, txnOrStxn interface{}, other []transactions.SignedTxn, proto string) (dr v2.DryrunRequest, err error) {
	gdr, err := MakeDryrunStateGenerated(client, txnOrStxn, other, proto)
	if err != nil {
		return
	}
	return v2.DryrunRequestFromGenerated(&gdr)
}

func MakeDryrunStateGenerated(client Client, txnOrStxn interface{}, other []transactions.SignedTxn, proto string) (dr generatedV2.DryrunRequest, err error) {
	var txns []transactions.SignedTxn
	if txnOrStxn == nil {
	} else if txn, ok := txnOrStxn.(transactions.Transaction); ok {
		txns = append(txns, transactions.SignedTxn{Txn: txn})
	} else if stxn, ok := txnOrStxn.(transactions.SignedTxn); ok {
		txns = append(txns, stxn)
	} else {
		err = fmt.Errorf("unsupported txn type")
		return
	}

	txns = append(txns, other...)
	for i := range txns {
		enc := protocol.EncodeJSON(&txns[i])
		dr.Txns = append(dr.Txns, enc)
	}

	for _, txn := range txns {
		tx := txn.Txn
		if tx.Type == protocol.ApplicationCallTx {
			apps := []basics.AppIndex{tx.ApplicationID}
			apps = append(apps, tx.ForeignApps...)
			for _, appIdx := range apps {
				var appParams generatedV2.ApplicationParams
				if appIdx == 0 {
					appParams.ApprovalProgram = tx.ApprovalProgram
					appParams.ClearStateProgram = tx.ClearStateProgram
					appParams.GlobalStateSchema = &generatedV2.ApplicationStateSchema{
						NumUint:      tx.GlobalStateSchema.NumUint,
						NumByteSlice: tx.GlobalStateSchema.NumByteSlice,
					}
					appParams.LocalStateSchema = &generatedV2.ApplicationStateSchema{
						NumUint:      tx.LocalStateSchema.NumUint,
						NumByteSlice: tx.LocalStateSchema.NumByteSlice,
					}
					appParams.Creator = tx.Sender.String()
					appIdx = defaultAppIdx
				} else {
					var app generatedV2.Application
					if app, err = client.ApplicationInformation(uint64(tx.ApplicationID)); err != nil {
						return
					}
					appParams = app.Params
				}
				dr.Apps = append(dr.Apps, generatedV2.Application{
					Id:     uint64(appIdx),
					Params: appParams,
				})
			}

			accounts := append(tx.Accounts, tx.Sender)
			for _, acc := range accounts {
				var info generatedV2.Account
				if info, err = client.AccountInformationV2(acc.String()); err != nil {
					return
				}
				dr.Accounts = append(dr.Accounts, info)
			}

			dr.ProtocolVersion = proto
			if dr.Round, err = client.CurrentRound(); err != nil {
				return
			}
			var b v1.Block
			if b, err = client.Block(dr.Round); err != nil {
				return
			}
			dr.LatestTimestamp = uint64(b.Timestamp)
		}
	}
	return
}

func (c *Client) Dryrun(data []byte) (resp generatedV2.DryrunResponse, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		data, err = algod.RawDryrun(data)
		if err != nil {
			return
		}
		err = json.Unmarshal(data, &resp)
	}
	return
}

func (c *Client) TxnProof(txid string, round uint64) (resp generatedV2.ProofResponse, err error) {
	algod, err := c.ensureAlgodClient()
	if err == nil {
		return algod.Proof(txid, round)
	}
	return
}
