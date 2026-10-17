package protocol

import (
	"bytes"
	"strings"
	"testing"
)

func TestWebSocketFrameEncodeDecodeUnmasked(t *testing.T) {
	original := &Frame{
		Fin:     true,
		Opcode:  OpcodeText,
		Masked:  false,
		Payload: []byte("HELLO_WEBSOCKET"),
	}

	encoded := EncodeFrame(original)
	decoded, err := ReadFrame(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("failed to decode unmasked frame: %v", err)
	}

	if !decoded.Fin {
		t.Errorf("expected Fin=true")
	}
	if decoded.Opcode != OpcodeText {
		t.Errorf("expected OpcodeText, got %v", decoded.Opcode)
	}
	if decoded.Masked {
		t.Errorf("expected Masked=false")
	}
	if string(decoded.Payload) != "HELLO_WEBSOCKET" {
		t.Errorf("payload mismatch: expected HELLO_WEBSOCKET, got %s", string(decoded.Payload))
	}
}

func TestWebSocketFrameEncodeDecodeMasked(t *testing.T) {
	maskKey := [4]byte{0x1A, 0x2B, 0x3C, 0x4D}
	original := &Frame{
		Fin:     true,
		Opcode:  OpcodeText,
		Masked:  true,
		MaskKey: maskKey,
		Payload: []byte("JOIN Alice room_42\n"),
	}

	encoded := EncodeFrame(original)
	decoded, err := ReadFrame(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("failed to decode masked frame: %v", err)
	}

	if !decoded.Masked {
		t.Errorf("expected Masked=true")
	}
	if decoded.MaskKey != maskKey {
		t.Errorf("mask key mismatch: expected %v, got %v", maskKey, decoded.MaskKey)
	}
	if string(decoded.Payload) != "JOIN Alice room_42\n" {
		t.Errorf("unmasked payload mismatch: expected %q, got %q", "JOIN Alice room_42\n", string(decoded.Payload))
	}
}

func TestWebSocketFrameExtended16BitLength(t *testing.T) {
	largePayload := bytes.Repeat([]byte("A"), 1024) // 1024 bytes requires 16-bit length
	original := &Frame{
		Fin:     true,
		Opcode:  OpcodeBinary,
		Masked:  false,
		Payload: largePayload,
	}

	encoded := EncodeFrame(original)
	decoded, err := ReadFrame(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("failed to decode extended length frame: %v", err)
	}

	if decoded.PayloadLen != 1024 {
		t.Errorf("expected payload length 1024, got %d", decoded.PayloadLen)
	}
	if !bytes.Equal(decoded.Payload, largePayload) {
		t.Errorf("extended payload content mismatch")
	}
}

func TestWebSocketControlFrames(t *testing.T) {
	ping := &Frame{
		Fin:     true,
		Opcode:  OpcodePing,
		Masked:  false,
		Payload: []byte("ping"),
	}
	if !ping.IsControlFrame() {
		t.Errorf("expected Ping to be identified as control frame")
	}

	encoded := EncodeFrame(ping)
	decoded, err := ReadFrame(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("failed to decode ping frame: %v", err)
	}
	if decoded.Opcode != OpcodePing {
		t.Errorf("expected OpcodePing, got %v", decoded.Opcode)
	}
}

func TestUnmaskInPlace(t *testing.T) {
	mask := [4]byte{0xDE, 0xAD, 0xBE, 0xEF}
	raw := []byte("The quick brown fox jumps over the lazy dog.")

	masked := make([]byte, len(raw))
	copy(masked, raw)
	UnmaskInPlace(masked, mask)

	if bytes.Equal(masked, raw) {
		t.Errorf("masked data should differ from original")
	}

	UnmaskInPlace(masked, mask) // XOR again should restore original
	if !bytes.Equal(masked, raw) {
		t.Errorf("second unmasking did not restore original text")
	}
}

func TestParseCard(t *testing.T) {
	tests := []struct {
		token     string
		wantColor CardColor
		wantValue CardValue
		wantErr   bool
	}{
		{"RED_7", ColorRed, ValSeven, false},
		{"blue_skip", ColorBlue, ValSkip, false},
		{"YELLOW_DRAW_TWO", ColorYellow, ValDrawTwo, false},
		{"YELLOW_DRAW2", ColorYellow, ValDrawTwo, false},
		{"GREEN_0", ColorGreen, ValZero, false},
		{"WILD", ColorWild, ValWild, false},
		{"WILD_WILD", ColorWild, ValWild, false},
		{"WILD_DRAW_FOUR", ColorWild, ValWildDrawFour, false},
		{"WILD_DRAW4", ColorWild, ValWildDrawFour, false},
		{"INVALID", "", "", true},
		{"", "", "", true},
		{"PURPLE_7", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.token, func(t *testing.T) {
			c, err := ParseCard(tt.token)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseCard(%q) error = %v, wantErr = %v", tt.token, err, tt.wantErr)
			}
			if !tt.wantErr {
				if c.Color != tt.wantColor {
					t.Errorf("color mismatch: expected %v, got %v", tt.wantColor, c.Color)
				}
				if c.Value != tt.wantValue {
					t.Errorf("value mismatch: expected %v, got %v", tt.wantValue, c.Value)
				}
			}
		})
	}
}

func TestFormatCard(t *testing.T) {
	c1 := Card{Color: ColorRed, Value: ValSeven}
	if f := FormatCard(c1); f != "RED_SEVEN" {
		t.Errorf("expected RED_SEVEN, got %s", f)
	}

	c2 := Card{Color: ColorWild, Value: ValWild}
	if f := FormatCard(c2); f != "WILD" {
		t.Errorf("expected WILD, got %s", f)
	}

	c3 := Card{Color: ColorWild, Value: ValWildDrawFour}
	if f := FormatCard(c3); f != "WILD_DRAW_FOUR" {
		t.Errorf("expected WILD_DRAW_FOUR, got %s", f)
	}
}

func TestParseUltimoMessage(t *testing.T) {
	// 1. Client Commands
	cmdLine := "PLAY RED_7\n"
	msg, err := ParseUltimoMessage(cmdLine)
	if err != nil {
		t.Fatalf("failed to parse command: %v", err)
	}
	if msg.Category != CategoryCommand {
		t.Errorf("expected CategoryCommand, got %v", msg.Category)
	}
	if msg.Verb != "PLAY" {
		t.Errorf("expected PLAY, got %s", msg.Verb)
	}
	if len(msg.Args) != 1 || msg.Args[0] != "RED_7" {
		t.Errorf("args mismatch: got %v", msg.Args)
	}

	// 2. Server Events
	evtLine := "TOP_CARD BLUE_5 BLUE\r\n"
	evt, err := ParseUltimoMessage(evtLine)
	if err != nil {
		t.Fatalf("failed to parse event: %v", err)
	}
	if evt.Category != CategoryEvent {
		t.Errorf("expected CategoryEvent, got %v", evt.Category)
	}
	if evt.Verb != "TOP_CARD" {
		t.Errorf("expected TOP_CARD, got %s", evt.Verb)
	}
	if len(evt.Args) != 2 || evt.Args[0] != "BLUE_5" || evt.Args[1] != "BLUE" {
		t.Errorf("args mismatch: got %v", evt.Args)
	}

	// 3. Empty input error
	if _, err := ParseUltimoMessage("   \n"); err == nil {
		t.Errorf("expected error on empty line")
	}
}

func TestInspect(t *testing.T) {
	// Case 1: WebSocket HTTP Handshake
	httpUpgrade := []byte("GET /chat HTTP/1.1\r\nHost: server.example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
	res1 := Inspect(httpUpgrade)
	if !res1.IsHTTPHandshake {
		t.Errorf("expected IsHTTPHandshake=true")
	}
	if !strings.Contains(res1.Summary, "WS_HANDSHAKE") {
		t.Errorf("expected WS_HANDSHAKE in summary, got %s", res1.Summary)
	}

	// Case 2: WebSocket Text Frame carrying Ultimo PLAY
	wsFrame := EncodeFrame(&Frame{
		Fin:     true,
		Opcode:  OpcodeText,
		Payload: []byte("PLAY BLUE_SKIP\n"),
	})
	res2 := Inspect(wsFrame)
	if !res2.IsWebSocket {
		t.Errorf("expected IsWebSocket=true")
	}
	if !res2.IsUltimoMessage {
		t.Errorf("expected IsUltimoMessage=true")
	}
	if res2.Summary != "WS:TEXT:PLAY" {
		t.Errorf("expected WS:TEXT:PLAY, got %s", res2.Summary)
	}

	// Case 3: Raw Ultimo ASCII TCP line
	rawUltimo := []byte("WELCOME session_999 Welcome to Ultimo!\n")
	res3 := Inspect(rawUltimo)
	if !res3.IsUltimoMessage {
		t.Errorf("expected IsUltimoMessage=true for raw line")
	}
	if res3.Summary != "ULTIMO:SERVER_EVENT:WELCOME" {
		t.Errorf("expected ULTIMO:SERVER_EVENT:WELCOME, got %s", res3.Summary)
	}

	// Case 4: Raw arbitrary binary bytes
	rawBin := []byte{0x00, 0x11, 0x22, 0x33}
	res4 := Inspect(rawBin)
	if !strings.HasPrefix(res4.Summary, "RAW_BYTES") {
		t.Errorf("expected RAW_BYTES summary, got %s", res4.Summary)
	}
}

func BenchmarkUnmaskInPlace(b *testing.B) {
	payload := make([]byte, 4096)
	mask := [4]byte{0x12, 0x34, 0x56, 0x78}
	b.ReportAllocs()
	b.SetBytes(4096)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		UnmaskInPlace(payload, mask)
	}
}
