package protocol

import (
	"bytes"
	"testing"
)

// BenchmarkUnmaskInPlace_64B benchmarks XOR unmasking on small payloads (64 bytes).
func BenchmarkUnmaskInPlace_64B(b *testing.B) {
	benchmarkUnmask(b, 64)
}

// BenchmarkUnmaskInPlace_1KB benchmarks XOR unmasking on medium payloads (1 KB).
func BenchmarkUnmaskInPlace_1KB(b *testing.B) {
	benchmarkUnmask(b, 1024)
}

// BenchmarkUnmaskInPlace_32KB benchmarks XOR unmasking on buffer-pool sized payloads (32 KB).
func BenchmarkUnmaskInPlace_32KB(b *testing.B) {
	benchmarkUnmask(b, 32*1024)
}

// BenchmarkUnmaskInPlace_64KB benchmarks XOR unmasking on large payloads (64 KB).
func BenchmarkUnmaskInPlace_64KB(b *testing.B) {
	benchmarkUnmask(b, 64*1024)
}

func benchmarkUnmask(b *testing.B, size int) {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i & 0xFF)
	}
	mask := [4]byte{0xDE, 0xAD, 0xBE, 0xEF}

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		UnmaskInPlace(payload, mask)
	}
}

// BenchmarkWebSocketFrameDecode measures decoding time of a complete WebSocket frame.
func BenchmarkWebSocketFrameDecode(b *testing.B) {
	frame := &Frame{
		Fin:     true,
		Opcode:  OpcodeText,
		Masked:  true,
		MaskKey: [4]byte{0x12, 0x34, 0x56, 0x78},
		Payload: []byte("PLAY RED_7"),
	}
	raw := EncodeFrame(frame)
	reader := bytes.NewReader(raw)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader.Reset(raw)
		_, err := ReadFrame(reader)
		if err != nil {
			b.Fatalf("unexpected decode error: %v", err)
		}
	}
}

// BenchmarkWebSocketFrameEncode measures frame wire serialization time.
func BenchmarkWebSocketFrameEncode(b *testing.B) {
	frame := &Frame{
		Fin:     true,
		Opcode:  OpcodeText,
		Masked:  true,
		MaskKey: [4]byte{0x12, 0x34, 0x56, 0x78},
		Payload: []byte("PLAY RED_7"),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = EncodeFrame(frame)
	}
}

// BenchmarkUltimoParseMessage_Play measures line protocol tokenization for PLAY commands.
func BenchmarkUltimoParseMessage_Play(b *testing.B) {
	line := "PLAY RED_7\n"
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := ParseUltimoMessage(line)
		if err != nil {
			b.Fatalf("unexpected parse error: %v", err)
		}
	}
}

// BenchmarkUltimoParseMessage_Join measures line protocol tokenization for JOIN commands.
func BenchmarkUltimoParseMessage_Join(b *testing.B) {
	line := "JOIN Alice room_olympus\n"
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := ParseUltimoMessage(line)
		if err != nil {
			b.Fatalf("unexpected parse error: %v", err)
		}
	}
}

// BenchmarkParseCard measures strongly-typed card token decoding.
func BenchmarkParseCard(b *testing.B) {
	token := "YELLOW_DRAW_TWO"
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := ParseCard(token)
		if err != nil {
			b.Fatalf("unexpected parse error: %v", err)
		}
	}
}

// BenchmarkFormatCard measures strongly-typed card wire formatting.
func BenchmarkFormatCard(b *testing.B) {
	card := Card{Color: ColorYellow, Value: ValDrawTwo}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = FormatCard(card)
	}
}

// BenchmarkInspect_WebSocket measures L7 inspection latency on WebSocket frames.
func BenchmarkInspect_WebSocket(b *testing.B) {
	data := []byte{0x81, 0x0A, 'P', 'L', 'A', 'Y', ' ', 'R', 'E', 'D', '_', '7'}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		res := Inspect(data)
		if !res.IsWebSocket {
			b.Fatal("expected websocket detection")
		}
	}
}

// BenchmarkInspect_Raw measures L7 inspection latency on raw L4 payloads (fastest rejection path).
func BenchmarkInspect_Raw(b *testing.B) {
	data := []byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = Inspect(data)
	}
}
