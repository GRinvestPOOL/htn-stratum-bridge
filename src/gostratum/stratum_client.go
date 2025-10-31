package gostratum

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func spawnClientListener(ctx *StratumContext, connection net.Conn, s *StratumListener) error {
	defer ctx.Disconnect()
	
	protocolError := false
	for {
		err := readFromConnection(connection, func(line string) error {
			event, err := UnmarshalEvent(line)
			if err != nil {
				ctx.Logger.Error("error unmarshalling event", zap.String("raw", line))
				return err
			}
			return s.HandleEvent(ctx, event)
		})
		if errors.Is(err, os.ErrDeadlineExceeded) {
			continue // expected timeout
		}
		if ctx.Err() != nil {
			return ctx.Err() // context cancelled
		}
		if ctx.parentContext.Err() != nil {
			return ctx.parentContext.Err() // parent context cancelled
		}
		if err != nil { // actual error
			// Check if this looks like a protocol error (ADB, HTTP, etc.)
			if strings.Contains(err.Error(), "protocol detected") {
				protocolError = true
				ctx.Logger.Info("non-stratum protocol detected, disconnecting", zap.Error(err))
			} else {
				ctx.Logger.Error("error reading from socket", zap.Error(err))
			}
			
			// Mark IP for rate limiting if it's a protocol error
			if protocolError {
				s.connectionMutex.Lock()
				s.failedConnections[ctx.RemoteAddr] = time.Now()
				// Clean up old entries (older than 1 minute)
				for ip, timestamp := range s.failedConnections {
					if time.Since(timestamp) > time.Minute {
						delete(s.failedConnections, ip)
					}
				}
				s.connectionMutex.Unlock()
			}
			return err
		}
	}
}

type LineCallback func(line string) error

func readFromConnection(connection net.Conn, cb LineCallback) error {
	deadline := time.Now().Add(5 * time.Second).UTC()
	if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}

	buffer := make([]byte, 8096*2)
	n, err := connection.Read(buffer)
	if err != nil {
		return errors.Wrapf(err, "error reading from connection")
	}
	
	// Check for non-stratum protocols and reject them early
	if n >= 4 {
		// Check for ADB protocol (starts with "CNXN")
		if bytes.HasPrefix(buffer[:n], []byte("CNXN")) {
			return fmt.Errorf("ADB protocol detected, not a stratum client")
		}
		// Check for HTTP requests
		if bytes.HasPrefix(buffer[:n], []byte("GET ")) || 
		   bytes.HasPrefix(buffer[:n], []byte("POST ")) ||
		   bytes.HasPrefix(buffer[:n], []byte("HEAD ")) {
			return fmt.Errorf("HTTP protocol detected, not a stratum client")
		}
	}
	
	buffer = bytes.ReplaceAll(buffer[:n], []byte("\x00"), nil)
	scanner := bufio.NewScanner(strings.NewReader(string(buffer)))
	for scanner.Scan() {
		if err := cb(scanner.Text()); err != nil {
			return err
		}
	}
	return nil
}
