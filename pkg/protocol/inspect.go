package protocol

import (
	"bytes"
	"fmt"
	"strings"
)

// InspectionResult encapsulates L7 metadata extracted from in-flight packet buffers.
type InspectionResult struct {
	IsWebSocket     bool
	IsHTTPHandshake bool
	IsUltimoMessage bool
	Frame           *Frame
	UltimoMsg       *UltimoMessage
	Summary         string
}

// Inspect inspects an arbitrary buffer chunk and extracts protocol semantics without modifying data.
func Inspect(data []byte) *InspectionResult {
	res := &InspectionResult{
		Summary: "L4_RAW",
	}

	if len(data) == 0 {
		return res
	}

	// 1. Check for HTTP / WebSocket Upgrade Handshake
	if bytes.HasPrefix(data, []byte("GET ")) || bytes.HasPrefix(data, []byte("HTTP/1.1 ")) {
		headerStr := string(data[:min(len(data), 512)])
		if strings.Contains(strings.ToLower(headerStr), "upgrade: websocket") ||
			strings.Contains(strings.ToLower(headerStr), "101 switching protocols") {
			res.IsHTTPHandshake = true
			res.Summary = "HTTP:WS_HANDSHAKE"
			return res
		}
	}

	// 2. Check for RFC 6455 WebSocket Frame
	if len(data) >= 2 {
		r := bytes.NewReader(data)
		if frame, err := ReadFrame(r); err == nil {
			res.IsWebSocket = true
			res.Frame = frame

			var opcodeName string
			switch frame.Opcode {
			case OpcodeText:
				opcodeName = "TEXT"
			case OpcodeBinary:
				opcodeName = "BINARY"
			case OpcodeClose:
				opcodeName = "CLOSE"
			case OpcodePing:
				opcodeName = "PING"
			case OpcodePong:
				opcodeName = "PONG"
			default:
				opcodeName = fmt.Sprintf("OP_0x%X", frame.Opcode)
			}

			// If text payload inside WS frame, attempt to parse as Ultimo command
			if frame.Opcode == OpcodeText && len(frame.Payload) > 0 {
				if umsg, uerr := ParseUltimoMessage(string(frame.Payload)); uerr == nil {
					res.IsUltimoMessage = true
					res.UltimoMsg = umsg
					res.Summary = fmt.Sprintf("WS:%s:%s", opcodeName, umsg.Verb)
					return res
				}
			}

			res.Summary = fmt.Sprintf("WS:%s(%d bytes)", opcodeName, frame.PayloadLen)
			return res
		}
	}

	// 3. Check for Direct ASCII Line Ultimo Protocol
	line := string(data)
	if umsg, err := ParseUltimoMessage(line); err == nil && umsg.Category != CategoryUnknown {
		res.IsUltimoMessage = true
		res.UltimoMsg = umsg
		res.Summary = fmt.Sprintf("ULTIMO:%s:%s", umsg.Category, umsg.Verb)
		return res
	}

	res.Summary = fmt.Sprintf("RAW_BYTES(%d)", len(data))
	return res
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
