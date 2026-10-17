package engine

import (
	"net"
)

// ApplyTCPReset forces an immediate hardware TCP reset (RST) on a connection, bypassing FIN handshake.
func ApplyTCPReset(conn net.Conn) error {
	if conn == nil {
		return nil
	}

	if tcp, ok := conn.(*net.TCPConn); ok {
		// SetLinger(0) causes subsequent Close() to discard unread data and send TCP RST
		_ = tcp.SetLinger(0)
		return tcp.Close()
	}

	return conn.Close()
}

// ApplyGracefulClose performs a standard clean socket termination.
func ApplyGracefulClose(conn net.Conn) error {
	if conn == nil {
		return nil
	}
	return conn.Close()
}
