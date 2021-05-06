
package transactions

import (
	"fmt"

	"github.com/algorand/go-algorand/config"
	"github.com/algorand/go-algorand/crypto"
	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/protocol"
)

type Txid crypto.Digest

func (txid Txid) String() string {
	return fmt.Sprintf("%v", crypto.Digest(txid))
}

func (txid *Txid) UnmarshalText(text []byte) error {
	d, err := crypto.DigestFromString(string(text))
	*txid = Txid(d)
	return err
}

type SpecialAddresses struct {
	FeeSink     basics.Address
	RewardsPool basics.Address
}

type Header struct {
	_struct struct{} `codec:",omitempty,omitemptyarray"`

	Sender      basics.Address    `codec:"snd"`
	Fee         basics.MicroAlgos `codec:"fee"`
	FirstValid  basics.Round      `codec:"fv"`
	LastValid   basics.Round      `codec:"lv"`
	Note        []byte            `codec:"note,allocbound=config.MaxTxnNoteBytes"` // Uniqueness or app-level data about txn
	GenesisID   string            `codec:"gen"`
	GenesisHash crypto.Digest     `codec:"gh"`

	Group crypto.Digest `codec:"grp"`

	Lease [32]byte `codec:"lx"`

	RekeyTo basics.Address `codec:"rekey"`
}

type Transaction struct {
	_struct struct{} `codec:",omitempty,omitemptyarray"`

	Type protocol.TxType `codec:"type"`

	Header

	KeyregTxnFields
	PaymentTxnFields
	AssetConfigTxnFields
	AssetTransferTxnFields
	AssetFreezeTxnFields
	ApplicationCallTxnFields
	CompactCertTxnFields
}

type ApplyData struct {
	_struct struct{} `codec:",omitempty,omitemptyarray"`

	ClosingAmount basics.MicroAlgos `codec:"ca"`

	AssetClosingAmount uint64 `codec:"aca"`

	SenderRewards   basics.MicroAlgos `codec:"rs"`
	ReceiverRewards basics.MicroAlgos `codec:"rr"`
	CloseRewards    basics.MicroAlgos `codec:"rc"`
	EvalDelta       basics.EvalDelta  `codec:"dt"`
}

func (ad ApplyData) Equal(o ApplyData) bool {
	if ad.ClosingAmount != o.ClosingAmount {
		return false
	}
	if ad.AssetClosingAmount != o.AssetClosingAmount {
		return false
	}
	if ad.SenderRewards != o.SenderRewards {
		return false
	}
	if ad.ReceiverRewards != o.ReceiverRewards {
		return false
	}
	if ad.CloseRewards != o.CloseRewards {
		return false
	}
	if !ad.EvalDelta.Equal(o.EvalDelta) {
		return false
	}
	return true
}

type TxGroup struct {
	_struct struct{} `codec:",omitempty,omitemptyarray"`

	TxGroupHashes []crypto.Digest `codec:"txlist,allocbound=config.MaxTxGroupSize"`
}

func (tg TxGroup) ToBeHashed() (protocol.HashID, []byte) {
	return protocol.TxGroup, protocol.Encode(&tg)
}

func (tx Transaction) ToBeHashed() (protocol.HashID, []byte) {
	return protocol.Transaction, protocol.Encode(&tx)
}

func (tx Transaction) ID() Txid {
	enc := tx.MarshalMsg(append(protocol.GetEncodingBuf(), []byte(protocol.Transaction)...))
	defer protocol.PutEncodingBuf(enc)
	return Txid(crypto.Hash(enc))
}

func (tx Transaction) Sign(secrets *crypto.SignatureSecrets) SignedTxn {
	sig := secrets.Sign(tx)

	s := SignedTxn{
		Txn: tx,
		Sig: sig,
	}
	if basics.Address(secrets.SignatureVerifier) != tx.Sender {
		s.AuthAddr = basics.Address(secrets.SignatureVerifier)
	}
	return s
}

func (tx Header) Src() basics.Address {
	return tx.Sender
}

func (tx Header) TxFee() basics.MicroAlgos {
	return tx.Fee
}

func (tx Header) Alive(tc TxnContext) error {
	round := tc.Round()
	if round < tx.FirstValid || round > tx.LastValid {
		return TxnDeadError{
			Round:      round,
			FirstValid: tx.FirstValid,
			LastValid:  tx.LastValid,
		}
	}

	proto := tc.ConsensusProtocol()
	genesisID := tc.GenesisID()
	if tx.GenesisID != "" && tx.GenesisID != genesisID {
		return fmt.Errorf("tx.GenesisID <%s> does not match expected <%s>",
			tx.GenesisID, genesisID)
	}

	if proto.SupportGenesisHash {
		genesisHash := tc.GenesisHash()
		if tx.GenesisHash != (crypto.Digest{}) && tx.GenesisHash != genesisHash {
			return fmt.Errorf("tx.GenesisHash <%s> does not match expected <%s>",
				tx.GenesisHash, genesisHash)
		}
		if proto.RequireGenesisHash && tx.GenesisHash == (crypto.Digest{}) {
			return fmt.Errorf("required tx.GenesisHash is missing")
		}
	} else {
		if tx.GenesisHash != (crypto.Digest{}) {
			return fmt.Errorf("tx.GenesisHash <%s> not allowed", tx.GenesisHash)
		}
	}

	return nil
}

func (tx Transaction) MatchAddress(addr basics.Address, spec SpecialAddresses) bool {
	for _, candidate := range tx.RelevantAddrs(spec) {
		if addr == candidate {
			return true
		}
	}
	return false
}

func (tx Transaction) WellFormed(spec SpecialAddresses, proto config.ConsensusParams) error {
	switch tx.Type {
	case protocol.PaymentTx:
		err := tx.checkSpender(tx.Header, spec, proto)
		if err != nil {
			return err
		}

	case protocol.KeyRegistrationTx:
		if tx.KeyregTxnFields.Nonparticipation {
			if !proto.SupportBecomeNonParticipatingTransactions {
				return fmt.Errorf("transaction tries to mark an account as nonparticipating, but that transaction is not supported")
			}
			suppliesNullKeys := tx.KeyregTxnFields.VotePK == crypto.OneTimeSignatureVerifier{} || tx.KeyregTxnFields.SelectionPK == crypto.VRFVerifier{}
			if !suppliesNullKeys {
				return fmt.Errorf("transaction tries to register keys to go online, but nonparticipatory flag is set")
			}
		}

	case protocol.AssetConfigTx:
		if !proto.Asset {
			return fmt.Errorf("asset transaction not supported")
		}

	case protocol.AssetTransferTx:
		if !proto.Asset {
			return fmt.Errorf("asset transaction not supported")
		}

	case protocol.AssetFreezeTx:
		if !proto.Asset {
			return fmt.Errorf("asset transaction not supported")
		}
	case protocol.ApplicationCallTx:
		if !proto.Application {
			return fmt.Errorf("application transaction not supported")
		}

		switch tx.OnCompletion {
		case NoOpOC:
		case OptInOC:
		case CloseOutOC:
		case ClearStateOC:
		case UpdateApplicationOC:
		case DeleteApplicationOC:
		default:
			return fmt.Errorf("invalid application OnCompletion")
		}

		if tx.ApplicationID != 0 && tx.OnCompletion != UpdateApplicationOC {
			if len(tx.ApprovalProgram) != 0 || len(tx.ClearStateProgram) != 0 {
				return fmt.Errorf("programs may only be specified during application creation or update")
			}
		}

		if tx.ApplicationID != 0 {
			if tx.LocalStateSchema != (basics.StateSchema{}) ||
				tx.GlobalStateSchema != (basics.StateSchema{}) {
				return fmt.Errorf("local and global state schemas are immutable")
			}
		}

		if len(tx.ApplicationArgs) > proto.MaxAppArgs {
			return fmt.Errorf("too many application args, max %d", proto.MaxAppArgs)
		}

		var argSum uint64
		for _, arg := range tx.ApplicationArgs {
			argSum = basics.AddSaturate(argSum, uint64(len(arg)))
		}

		if argSum > uint64(proto.MaxAppTotalArgLen) {
			return fmt.Errorf("application args total length too long, max len %d bytes", proto.MaxAppTotalArgLen)
		}

		if len(tx.Accounts) > proto.MaxAppTxnAccounts {
			return fmt.Errorf("tx.Accounts too long, max number of accounts is %d", proto.MaxAppTxnAccounts)
		}

		if len(tx.ForeignApps) > proto.MaxAppTxnForeignApps {
			return fmt.Errorf("tx.ForeignApps too long, max number of foreign apps is %d", proto.MaxAppTxnForeignApps)
		}

		if len(tx.ForeignAssets) > proto.MaxAppTxnForeignAssets {
			return fmt.Errorf("tx.ForeignAssets too long, max number of foreign assets is %d", proto.MaxAppTxnForeignAssets)
		}

		if len(tx.ApprovalProgram) > proto.MaxAppProgramLen {
			return fmt.Errorf("approval program too long. max len %d bytes", proto.MaxAppProgramLen)
		}

		if len(tx.ClearStateProgram) > proto.MaxAppProgramLen {
			return fmt.Errorf("clear state program too long. max len %d bytes", proto.MaxAppProgramLen)
		}

		if tx.LocalStateSchema.NumEntries() > proto.MaxLocalSchemaEntries {
			return fmt.Errorf("tx.LocalStateSchema too large, max number of keys is %d", proto.MaxLocalSchemaEntries)
		}

		if tx.GlobalStateSchema.NumEntries() > proto.MaxGlobalSchemaEntries {
			return fmt.Errorf("tx.GlobalStateSchema too large, max number of keys is %d", proto.MaxGlobalSchemaEntries)
		}

	case protocol.CompactCertTx:
		if proto.CompactCertRounds == 0 {
			return fmt.Errorf("compact certs not supported")
		}

		if tx.Sender != CompactCertSender {
			return fmt.Errorf("sender must be the compact-cert sender")
		}
		if !tx.Fee.IsZero() {
			return fmt.Errorf("fee must be zero")
		}
		if len(tx.Note) != 0 {
			return fmt.Errorf("note must be empty")
		}
		if !tx.Group.IsZero() {
			return fmt.Errorf("group must be zero")
		}
		if !tx.RekeyTo.IsZero() {
			return fmt.Errorf("rekey must be zero")
		}
		if tx.Lease != [32]byte{} {
			return fmt.Errorf("lease must be zero")
		}

	default:
		return fmt.Errorf("unknown tx type %v", tx.Type)
	}

	nonZeroFields := make(map[protocol.TxType]bool)
	if tx.PaymentTxnFields != (PaymentTxnFields{}) {
		nonZeroFields[protocol.PaymentTx] = true
	}

	if tx.KeyregTxnFields != (KeyregTxnFields{}) {
		nonZeroFields[protocol.KeyRegistrationTx] = true
	}

	if tx.AssetConfigTxnFields != (AssetConfigTxnFields{}) {
		nonZeroFields[protocol.AssetConfigTx] = true
	}

	if tx.AssetTransferTxnFields != (AssetTransferTxnFields{}) {
		nonZeroFields[protocol.AssetTransferTx] = true
	}

	if tx.AssetFreezeTxnFields != (AssetFreezeTxnFields{}) {
		nonZeroFields[protocol.AssetFreezeTx] = true
	}

	if !tx.ApplicationCallTxnFields.Empty() {
		nonZeroFields[protocol.ApplicationCallTx] = true
	}

	if !tx.CompactCertTxnFields.Empty() {
		nonZeroFields[protocol.CompactCertTx] = true
	}

	for t, nonZero := range nonZeroFields {
		if nonZero && t != tx.Type {
			return fmt.Errorf("transaction of type %v has non-zero fields for type %v", tx.Type, t)
		}
	}

	if tx.Fee.LessThan(basics.MicroAlgos{Raw: proto.MinTxnFee}) {
		if tx.Type == protocol.CompactCertTx {
		} else {
			return makeMinFeeErrorf("transaction had fee %d, which is less than the minimum %d", tx.Fee.Raw, proto.MinTxnFee)
		}
	}
	if tx.LastValid < tx.FirstValid {
		return fmt.Errorf("transaction invalid range (%v--%v)", tx.FirstValid, tx.LastValid)
	}
	if tx.LastValid-tx.FirstValid > basics.Round(proto.MaxTxnLife) {
		return fmt.Errorf("transaction window size excessive (%v--%v)", tx.FirstValid, tx.LastValid)
	}
	if len(tx.Note) > proto.MaxTxnNoteBytes {
		return fmt.Errorf("transaction note too big: %d > %d", len(tx.Note), proto.MaxTxnNoteBytes)
	}
	if len(tx.AssetConfigTxnFields.AssetParams.AssetName) > proto.MaxAssetNameBytes {
		return fmt.Errorf("transaction asset name too big: %d > %d", len(tx.AssetConfigTxnFields.AssetParams.AssetName), proto.MaxAssetNameBytes)
	}
	if len(tx.AssetConfigTxnFields.AssetParams.UnitName) > proto.MaxAssetUnitNameBytes {
		return fmt.Errorf("transaction asset unit name too big: %d > %d", len(tx.AssetConfigTxnFields.AssetParams.UnitName), proto.MaxAssetUnitNameBytes)
	}
	if len(tx.AssetConfigTxnFields.AssetParams.URL) > proto.MaxAssetURLBytes {
		return fmt.Errorf("transaction asset url too big: %d > %d", len(tx.AssetConfigTxnFields.AssetParams.URL), proto.MaxAssetURLBytes)
	}
	if tx.AssetConfigTxnFields.AssetParams.Decimals > proto.MaxAssetDecimals {
		return fmt.Errorf("transaction asset decimals is too high (max is %d)", proto.MaxAssetDecimals)
	}
	if tx.Sender == spec.RewardsPool {
		return fmt.Errorf("transaction from incentive pool is invalid")
	}
	if tx.Sender.IsZero() {
		return fmt.Errorf("transaction cannot have zero sender")
	}
	if !proto.SupportTransactionLeases && (tx.Lease != [32]byte{}) {
		return fmt.Errorf("transaction tried to acquire lease %v but protocol does not support transaction leases", tx.Lease)
	}
	if !proto.SupportTxGroups && (tx.Group != crypto.Digest{}) {
		return fmt.Errorf("transaction has group but groups not yet enabled")
	}
	if !proto.SupportRekeying && (tx.RekeyTo != basics.Address{}) {
		return fmt.Errorf("transaction has RekeyTo set but rekeying not yet enabled")
	}
	return nil
}

func (tx Header) Aux() []byte {
	return tx.Note
}

func (tx Header) First() basics.Round {
	return tx.FirstValid
}

func (tx Header) Last() basics.Round {
	return tx.LastValid
}

func (tx Transaction) RelevantAddrs(spec SpecialAddresses) []basics.Address {
	addrs := []basics.Address{tx.Sender, spec.FeeSink}

	switch tx.Type {
	case protocol.PaymentTx:
		addrs = append(addrs, tx.PaymentTxnFields.Receiver)
		if !tx.PaymentTxnFields.CloseRemainderTo.IsZero() {
			addrs = append(addrs, tx.PaymentTxnFields.CloseRemainderTo)
		}
	case protocol.AssetTransferTx:
		addrs = append(addrs, tx.AssetTransferTxnFields.AssetReceiver)
		if !tx.AssetTransferTxnFields.AssetCloseTo.IsZero() {
			addrs = append(addrs, tx.AssetTransferTxnFields.AssetCloseTo)
		}
		if !tx.AssetTransferTxnFields.AssetSender.IsZero() {
			addrs = append(addrs, tx.AssetTransferTxnFields.AssetSender)
		}
	}

	return addrs
}

func (tx Transaction) TxAmount() basics.MicroAlgos {
	switch tx.Type {
	case protocol.PaymentTx:
		return tx.PaymentTxnFields.Amount

	default:
		return basics.MicroAlgos{Raw: 0}
	}
}

func (tx Transaction) GetReceiverAddress() basics.Address {
	switch tx.Type {
	case protocol.PaymentTx:
		return tx.PaymentTxnFields.Receiver
	case protocol.AssetTransferTx:
		return tx.AssetTransferTxnFields.AssetReceiver
	default:
		return basics.Address{}
	}
}

func (tx Transaction) EstimateEncodedSize() int {
	stx := SignedTxn{
		Txn: tx,
		Sig: crypto.Signature{1},
	}
	return stx.GetEncodedLength()
}

type TxnContext interface {
	Round() basics.Round
	ConsensusProtocol() config.ConsensusParams
	GenesisID() string
	GenesisHash() crypto.Digest
}

type ExplicitTxnContext struct {
	ExplicitRound basics.Round
	Proto         config.ConsensusParams
	GenID         string
	GenHash       crypto.Digest
}

func (tc ExplicitTxnContext) Round() basics.Round {
	return tc.ExplicitRound
}

func (tc ExplicitTxnContext) ConsensusProtocol() config.ConsensusParams {
	return tc.Proto
}

func (tc ExplicitTxnContext) GenesisID() string {
	return tc.GenID
}

func (tc ExplicitTxnContext) GenesisHash() crypto.Digest {
	return tc.GenHash
}
