package protocol

// Opcode represents RFC 6455 WebSocket frame opcodes.
type Opcode byte

const (
	OpcodeContinuation Opcode = 0x0
	OpcodeText         Opcode = 0x1
	OpcodeBinary       Opcode = 0x2
	OpcodeClose        Opcode = 0x8
	OpcodePing         Opcode = 0x9
	OpcodePong         Opcode = 0xA
)

// UltimoCommand represents client-to-server commands in the authoritative Ultimo protocol.
type UltimoCommand string

const (
	CmdJoin  UltimoCommand = "JOIN"
	CmdStart UltimoCommand = "START"
	CmdPlay  UltimoCommand = "PLAY"
	CmdDraw  UltimoCommand = "DRAW"
	CmdPass  UltimoCommand = "PASS"
	CmdPing  UltimoCommand = "PING"
	CmdChat  UltimoCommand = "CHAT"
)

// UltimoEvent represents server-to-client events in the authoritative Ultimo protocol.
type UltimoEvent string

const (
	EventWelcome        UltimoEvent = "WELCOME"
	EventRoomStatus     UltimoEvent = "ROOM_STATUS"
	EventHandUpdate     UltimoEvent = "HAND_UPDATE"
	EventTopCard        UltimoEvent = "TOP_CARD"
	EventTurnAnnounced  UltimoEvent = "TURN_ANNOUNCED"
	EventOpponentUpdate UltimoEvent = "OPPONENT_UPDATE"
	EventDrawResult     UltimoEvent = "DRAW_RESULT"
	EventGameOver       UltimoEvent = "GAME_OVER"
	EventError          UltimoEvent = "ERROR"
	EventPong           UltimoEvent = "PONG"
)
