package protocol

import (
	"bytes"
	"testing"
)

// FuzzReadFrame tests WebSocket frame decoding against arbitrary byte sequences.
func FuzzReadFrame(f *testing.F) {
	f.Add([]byte{0x81, 0x05, 'H', 'e', 'l', 'l', 'o'})
	f.Add([]byte{0x81, 0x85, 0x12, 0x34, 0x56, 0x78, 'H' ^ 0x12, 'e' ^ 0x34, 'l' ^ 0x56, 'l' ^ 0x78, 'o' ^ 0x12})
	f.Add([]byte{0x89, 0x00})
	f.Add([]byte{0x88, 0x02, 0x03, 0xE8})
	f.Add([]byte{0x82, 0x7E, 0x01, 0x00})
	f.Add([]byte{})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		frame, err := ReadFrame(r)
		if err == nil && frame != nil {
			if frame.Masked {
				UnmaskInPlace(frame.Payload, frame.MaskKey)
			}
		}
	})
}

// FuzzUltimoCommandParser tests Ultimo wire command tokenization against arbitrary strings.
func FuzzUltimoCommandParser(f *testing.F) {
	f.Add("JOIN Alice room_1\n")
	f.Add("PLAY RED_7\n")
	f.Add("DRAW\n")
	f.Add("PASS\n")
	f.Add("PING\n")
	f.Add("CHAT Hello World!\n")
	f.Add("WELCOME room_1 p1 RED_5 7\n")
	f.Add("ROOM_STATUS 4 WAITING\n")
	f.Add("TOP_CARD BLUE_SKIP\n")
	f.Add("TURN_ANNOUNCED Bob\n")
	f.Add("OPPONENT_UPDATE Charlie 3\n")
	f.Add("GAME_OVER Alice\n")
	f.Add("INVALID_COMMAND\n")
	f.Add("")
	f.Add("\n\n\n")
	f.Add("PLAY \x00\xFF\xFE\n")

	f.Fuzz(func(t *testing.T, line string) {
		_, _ = ParseUltimoMessage(line)
	})
}

// FuzzParseCard tests card string token parsing against arbitrary tokens.
func FuzzParseCard(f *testing.F) {
	f.Add("RED_0")
	f.Add("BLUE_SKIP")
	f.Add("GREEN_DRAW_TWO")
	f.Add("YELLOW_REVERSE")
	f.Add("WILD")
	f.Add("WILD_DRAW_FOUR")
	f.Add("PURPLE_9")
	f.Add("")
	f.Add("___")
	f.Add("RED_")
	f.Add("_5")
	f.Add("\x00")

	f.Fuzz(func(t *testing.T, token string) {
		card, err := ParseCard(token)
		if err == nil {
			formatted := FormatCard(card)
			if len(formatted) == 0 {
				t.Errorf("expected non-empty formatted card for valid card: %+v", card)
			}
		}
	})
}

// FuzzInspect tests unified protocol inspection against arbitrary byte payloads.
func FuzzInspect(f *testing.F) {
	f.Add([]byte("GET /chat HTTP/1.1\r\nUpgrade: websocket\r\n\r\n"))
	f.Add([]byte{0x81, 0x0A, 'P', 'L', 'A', 'Y', ' ', 'R', 'E', 'D', '_', '7'})
	f.Add([]byte("JOIN Bob room_alpha\n"))
	f.Add([]byte{0x00, 0x01, 0x02, 0x03, 0x04})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		res := Inspect(data)
		if res == nil {
			t.Fatal("Inspect returned nil result")
		}
		if res.Summary == "" {
			t.Fatal("Inspect returned empty Summary")
		}
	})
}
