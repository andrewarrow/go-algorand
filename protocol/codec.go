
package protocol

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/algorand/go-codec/codec"
	"github.com/algorand/msgp/msgp"
)

var CodecHandle *codec.MsgpackHandle

var JSONHandle *codec.JsonHandle

var JSONStrictHandle *codec.JsonHandle

type Decoder interface {
	Decode(objptr interface{}) error
}

func init() {
	CodecHandle = new(codec.MsgpackHandle)
	CodecHandle.ErrorIfNoField = true
	CodecHandle.ErrorIfNoArrayExpand = true
	CodecHandle.Canonical = true
	CodecHandle.RecursiveEmptyCheck = true
	CodecHandle.WriteExt = true
	CodecHandle.PositiveIntUnsigned = true
	CodecHandle.Raw = true

	JSONHandle = new(codec.JsonHandle)
	JSONHandle.ErrorIfNoField = true
	JSONHandle.ErrorIfNoArrayExpand = true
	JSONHandle.Canonical = true
	JSONHandle.RecursiveEmptyCheck = true
	JSONHandle.Indent = 2
	JSONHandle.HTMLCharsAsIs = true

	JSONStrictHandle = new(codec.JsonHandle)
	JSONStrictHandle.ErrorIfNoField = JSONHandle.ErrorIfNoField
	JSONStrictHandle.ErrorIfNoArrayExpand = JSONHandle.ErrorIfNoArrayExpand
	JSONStrictHandle.Canonical = JSONHandle.Canonical
	JSONStrictHandle.RecursiveEmptyCheck = JSONHandle.RecursiveEmptyCheck
	JSONStrictHandle.Indent = JSONHandle.Indent
	JSONStrictHandle.HTMLCharsAsIs = JSONHandle.HTMLCharsAsIs
	JSONStrictHandle.MapKeyAsString = true
}

type codecBytes struct {
	enc *codec.Encoder

	buf []byte
}

var codecBytesPool = sync.Pool{
	New: func() interface{} {
		return &codecBytes{
			enc: codec.NewEncoderBytes(nil, CodecHandle),
		}
	},
}

var codecStreamPool = sync.Pool{
	New: func() interface{} {
		return codec.NewEncoder(nil, CodecHandle)
	},
}

const initEncodeBufSize = 256

func EncodeReflect(obj interface{}) []byte {
	codecBytes := codecBytesPool.Get().(*codecBytes)
	codecBytes.buf = make([]byte, initEncodeBufSize)
	codecBytes.enc.ResetBytes(&codecBytes.buf)
	codecBytes.enc.MustEncode(obj)
	res := codecBytes.buf
	codecBytesPool.Put(codecBytes)
	return res
}

func EncodeMsgp(obj msgp.Marshaler) []byte {
	return obj.MarshalMsg(nil)
}

func Encode(obj msgp.Marshaler) []byte {
	if obj.CanMarshalMsg(obj) {
		return EncodeMsgp(obj)
	}

	fmt.Fprintf(os.Stderr, "Encoding %T using go-codec; stray embedded field?\n", obj)
	return EncodeReflect(obj)
}

func EncodeStream(w io.Writer, obj interface{}) {
	enc := codecStreamPool.Get().(*codec.Encoder)
	enc.Reset(w)
	enc.MustEncode(obj)
	codecStreamPool.Put(enc)
}

func EncodeJSON(obj interface{}) []byte {
	var b []byte
	enc := codec.NewEncoderBytes(&b, JSONHandle)
	enc.MustEncode(obj)
	return b
}

func EncodeJSONStrict(obj interface{}) []byte {
	var b []byte
	enc := codec.NewEncoderBytes(&b, JSONStrictHandle)
	enc.MustEncode(obj)
	return b
}

func DecodeReflect(b []byte, objptr interface{}) error {
	dec := codec.NewDecoderBytes(b, CodecHandle)
	return dec.Decode(objptr)
}

func DecodeMsgp(b []byte, objptr msgp.Unmarshaler) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = fmt.Errorf("DecodeMsgp: %v", x)
		}
	}()

	var rem []byte
	rem, err = objptr.UnmarshalMsg(b)
	if err != nil {
		return err
	}

	if false {
		if len(rem) != 0 {
			return fmt.Errorf("decoding left %d remaining bytes", len(rem))
		}
	}

	return nil
}

func Decode(b []byte, objptr msgp.Unmarshaler) error {
	if objptr.CanUnmarshalMsg(objptr) {
		return DecodeMsgp(b, objptr)
	}

	fmt.Fprintf(os.Stderr, "Decoding %T using go-codec; stray embedded field?\n", objptr)

	return DecodeReflect(b, objptr)
}

func DecodeStream(r io.Reader, objptr interface{}) error {
	dec := codec.NewDecoder(r, CodecHandle)
	return dec.Decode(objptr)
}

func DecodeJSON(b []byte, objptr interface{}) error {
	dec := codec.NewDecoderBytes(b, JSONHandle)
	return dec.Decode(objptr)
}

func NewEncoder(w io.Writer) *codec.Encoder {
	return codec.NewEncoder(w, CodecHandle)
}

func NewJSONEncoder(w io.Writer) *codec.Encoder {
	return codec.NewEncoder(w, JSONHandle)
}

func NewDecoder(r io.Reader) Decoder {
	return codec.NewDecoder(r, CodecHandle)
}

func NewJSONDecoder(r io.Reader) Decoder {
	return codec.NewDecoder(r, JSONHandle)
}

func NewDecoderBytes(b []byte) Decoder {
	return codec.NewDecoderBytes(b, CodecHandle)
}

var encodingPool = sync.Pool{
	New: func() interface{} {
		return []byte{}
	},
}

func GetEncodingBuf() []byte {
	return encodingPool.Get().([]byte)[:0]
}

func PutEncodingBuf(s []byte) {
	encodingPool.Put(s)
}
