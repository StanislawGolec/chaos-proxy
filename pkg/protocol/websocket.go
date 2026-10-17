package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var (
	ErrPayloadTooLarge = errors.New("websocket frame payload exceeds maximum allowed size")
	ErrInvalidHeader   = errors.New("invalid websocket frame header")
)

const (
	// MaxPayloadSize protects against denial-of-service memory allocations (10 MB).
	MaxPayloadSize = 10 * 1024 * 1024
)

// Frame represents an RFC 6455 WebSocket frame.
type Frame struct {
	Fin        bool
	Rsv        byte // 3-bit reserved field
	Opcode     Opcode
	Masked     bool
	MaskKey    [4]byte
	PayloadLen uint64
	Payload    []byte
}

// ReadFrame decodes a single RFC 6455 WebSocket frame from an io.Reader.
func ReadFrame(r io.Reader) (*Frame, error) {
	var head [2]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, err
	}

	fin := (head[0] & 0x80) != 0
	rsv := (head[0] & 0x70) >> 4
	opcode := Opcode(head[0] & 0x0F)

	masked := (head[1] & 0x80) != 0
	payloadLenField := head[1] & 0x7F

	var payloadLen uint64
	switch payloadLenField {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return nil, fmt.Errorf("reading 16-bit payload length: %w", err)
		}
		payloadLen = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return nil, fmt.Errorf("reading 64-bit payload length: %w", err)
		}
		payloadLen = binary.BigEndian.Uint64(ext[:])
	default:
		payloadLen = uint64(payloadLenField)
	}

	if payloadLen > MaxPayloadSize {
		return nil, fmt.Errorf("%w: %d bytes (limit: %d)", ErrPayloadTooLarge, payloadLen, MaxPayloadSize)
	}

	var maskKey [4]byte
	if masked {
		if _, err := io.ReadFull(r, maskKey[:]); err != nil {
			return nil, fmt.Errorf("reading 4-byte mask key: %w", err)
		}
	}

	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("reading frame payload: %w", err)
		}
		if masked {
			UnmaskInPlace(payload, maskKey)
		}
	}

	return &Frame{
		Fin:        fin,
		Rsv:        rsv,
		Opcode:     opcode,
		Masked:     masked,
		MaskKey:    maskKey,
		PayloadLen: payloadLen,
		Payload:    payload,
	}, nil
}

// EncodeFrame serializes a Frame into RFC 6455 wire format bytes.
func EncodeFrame(f *Frame) []byte {
	var buf []byte

	b0 := byte(f.Opcode & 0x0F)
	if f.Fin {
		b0 |= 0x80
	}
	b0 |= (f.Rsv & 0x07) << 4
	buf = append(buf, b0)

	var b1 byte
	if f.Masked {
		b1 |= 0x80
	}

	payloadLen := len(f.Payload)
	if payloadLen < 126 {
		b1 |= byte(payloadLen)
		buf = append(buf, b1)
	} else if payloadLen <= 65535 {
		b1 |= 126
		buf = append(buf, b1)
		var ext [2]byte
		binary.BigEndian.PutUint16(ext[:], uint16(payloadLen))
		buf = append(buf, ext[:]...)
	} else {
		b1 |= 127
		buf = append(buf, b1)
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(payloadLen))
		buf = append(buf, ext[:]...)
	}

	if f.Masked {
		buf = append(buf, f.MaskKey[:]...)
		maskedPayload := make([]byte, payloadLen)
		copy(maskedPayload, f.Payload)
		UnmaskInPlace(maskedPayload, f.MaskKey)
		buf = append(buf, maskedPayload...)
	} else {
		buf = append(buf, f.Payload...)
	}

	return buf
}

// UnmaskInPlace applies the 4-byte XOR masking key directly to the payload slice without heap allocation.
// Employs a 64-bit word loop for gigabytes-per-second memory throughput.
func UnmaskInPlace(payload []byte, maskKey [4]byte) {
	n := len(payload)
	if n == 0 {
		return
	}

	// Construct 64-bit repeated mask using register bit shifts (zero heap allocation)
	m0, m1, m2, m3 := uint64(maskKey[0]), uint64(maskKey[1]), uint64(maskKey[2]), uint64(maskKey[3])
	mask32 := m0 | (m1 << 8) | (m2 << 16) | (m3 << 24)
	mask64 := mask32 | (mask32 << 32)

	i := 0
	for ; i+8 <= n; i += 8 {
		w := binary.LittleEndian.Uint64(payload[i : i+8])
		binary.LittleEndian.PutUint64(payload[i:i+8], w^mask64)
	}

	// Residual byte loop
	for ; i < n; i++ {
		payload[i] ^= maskKey[i%4]
	}
}

// IsControlFrame returns true if opcode belongs to control frame category (Close, Ping, Pong).
func (f *Frame) IsControlFrame() bool {
	return f.Opcode == OpcodeClose || f.Opcode == OpcodePing || f.Opcode == OpcodePong
}
