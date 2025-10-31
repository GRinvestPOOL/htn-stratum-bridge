package gostratum

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type StratumContext struct {
	parentContext context.Context
	RemoteAddr    string
	WalletAddr    string
	WorkerName    string
	RemoteApp     string
	Id            int32
	Logger        *zap.Logger
	connection    net.Conn
	disconnecting bool
	onDisconnect  chan *StratumContext
	State         any // gross, but go generics aren't mature enough this can be typed 😭
	writeLock     int32
	Extranonce    string
	// Health monitoring fields
	lastWriteError    time.Time
	consecutiveErrors int32
	lastActivity      time.Time
}

type ContextSummary struct {
	RemoteAddr string
	WalletAddr string
	WorkerName string
	RemoteApp  string
}

var ErrorDisconnected = fmt.Errorf("disconnecting")

func (sc *StratumContext) Connected() bool {
	return !sc.disconnecting
}

func (sc *StratumContext) IsHealthy() bool {
	if sc.disconnecting {
		return false
	}
	
	// Check if too many consecutive errors
	if atomic.LoadInt32(&sc.consecutiveErrors) >= 5 {
		return false
	}
	
	// Check if client has been inactive for too long (5 minutes)
	if !sc.lastActivity.IsZero() && time.Since(sc.lastActivity) > 5*time.Minute {
		return false
	}
	
	return true
}

func (sc *StratumContext) Summary() ContextSummary {
	return ContextSummary{
		RemoteAddr: sc.RemoteAddr,
		WalletAddr: sc.WalletAddr,
		WorkerName: sc.WorkerName,
		RemoteApp:  sc.RemoteApp,
	}
}

func NewMockContext(ctx context.Context, logger *zap.Logger, state any) (*StratumContext, *MockConnection) {
	mc := NewMockConnection()
	return &StratumContext{
		parentContext: ctx,
		State:         state,
		RemoteAddr:    "127.0.0.1",
		WalletAddr:    uuid.NewString(),
		WorkerName:    uuid.NewString(),
		RemoteApp:     "mock.context",
		Logger:        logger,
		connection:    mc,
		lastActivity:  time.Now(),
	}, mc
}

func (sc *StratumContext) String() string {
	serialized, _ := json.Marshal(sc)
	return string(serialized)
}

func (sc *StratumContext) Reply(response JsonRpcResponse) error {
	if sc.disconnecting {
		return ErrorDisconnected
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return errors.Wrap(err, "failed encoding jsonrpc response")
	}
	encoded = append(encoded, '\n')
	return sc.writeWithBackoff(encoded)
}

func (sc *StratumContext) Send(event JsonRpcEvent) error {
	if sc.disconnecting {
		return ErrorDisconnected
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return errors.Wrap(err, "failed encoding jsonrpc event")
	}
	encoded = append(encoded, '\n')
	return sc.writeWithBackoff(encoded)
}

var errWriteBlocked = fmt.Errorf("error writing to socket, previous write pending")
var errTooManyErrors = fmt.Errorf("too many consecutive write errors, client unhealthy")

func (sc *StratumContext) write(data []byte) error {
	if atomic.CompareAndSwapInt32(&sc.writeLock, 0, 1) {
		defer atomic.StoreInt32(&sc.writeLock, 0)
		
		// Check if client has too many consecutive errors
		if atomic.LoadInt32(&sc.consecutiveErrors) >= 5 {
			sc.Logger.Warn("client has too many consecutive write errors, disconnecting")
			go sc.Disconnect()
			return errTooManyErrors
		}
		
		// Use shorter timeout to prevent blocking
		deadline := time.Now().Add(2 * time.Second)
		if err := sc.connection.SetWriteDeadline(deadline); err != nil {
			atomic.AddInt32(&sc.consecutiveErrors, 1)
			sc.lastWriteError = time.Now()
			return errors.Wrap(err, "failed setting write deadline for connection")
		}
		
		_, err := sc.connection.Write(data)
		if err != nil {
			atomic.AddInt32(&sc.consecutiveErrors, 1)
			sc.lastWriteError = time.Now()
			sc.checkDisconnect(err)
		} else {
			// Reset error counter on successful write
			atomic.StoreInt32(&sc.consecutiveErrors, 0)
			sc.lastActivity = time.Now()
		}
		return err
	}
	return errWriteBlocked
}

func (sc *StratumContext) writeWithBackoff(data []byte) error {
	// Fast fail if client is already marked as unhealthy
	if sc.disconnecting {
		return ErrorDisconnected
	}
	
	for i := 0; i < 3; i++ {
		err := sc.write(data)
		if err == nil {
			return nil
		} else if err == errWriteBlocked {
			// Use exponential backoff instead of fixed delay
			backoffDelay := time.Duration(1<<uint(i)) * time.Millisecond
			time.Sleep(backoffDelay)
			continue
		} else if err == errTooManyErrors {
			// Don't retry if client is marked unhealthy
			return err
		} else {
			// For other errors, retry once more then give up
			if i == 0 {
				time.Sleep(1 * time.Millisecond)
				continue
			}
			return err
		}
	}
	
	// After 3 attempts, mark client as problematic and schedule disconnect
	sc.Logger.Warn("failed writing to socket after 3 attempts, client may be unresponsive")
	atomic.StoreInt32(&sc.consecutiveErrors, 5) // Mark as unhealthy
	go sc.Disconnect() // Async disconnect to avoid blocking
	return fmt.Errorf("failed writing to socket after 3 attempts, client disconnected")
}

func (sc *StratumContext) ReplySuccess(id any) error {
	return sc.Reply(JsonRpcResponse{
		Id:     id,
		Result: true,
		Error:  nil,
	})
}

func (sc *StratumContext) ReplyStaleShare(id any) error {
	return sc.Reply(JsonRpcResponse{
		Id:     id,
		Result: nil,
		Error:  []any{21, "Job not found", nil},
	})
}

func (sc *StratumContext) ReplyDupeShare(id any) error {
	return sc.Reply(JsonRpcResponse{
		Id:     id,
		Result: nil,
		Error:  []any{22, "Duplicate share submitted", nil},
	})
}

func (sc *StratumContext) ReplyIncorrectData(id any) error {
	return sc.Reply(JsonRpcResponse{
		Id:     id,
		Result: nil,
		Error:  []any{20, "Incorrect submission data.", nil},
	})
}

func (sc *StratumContext) ReplyBadShare(id any) error {
	return sc.Reply(JsonRpcResponse{
		Id:     id,
		Result: nil,
		Error:  []any{20, "Bad share for unknown reason.", nil},
	})
}

func (sc *StratumContext) ReplyIncorrectPow(id any) error {
	return sc.Reply(JsonRpcResponse{
		Id:     id,
		Result: nil,
		Error:  []any{20, "Incorrect proof of work hash", nil},
	})
}

func (sc *StratumContext) ReplyLowDiffShare(id any) error {
	return sc.Reply(JsonRpcResponse{
		Id:     id,
		Result: nil,
		Error:  []any{23, "Invalid difficulty", nil},
	})
}

func (sc *StratumContext) Disconnect() {
	if !sc.disconnecting {
		sc.Logger.Info("disconnecting")
		sc.disconnecting = true
		if sc.connection != nil {
			sc.connection.Close()
		}
		sc.onDisconnect <- sc
	}
}

func (sc *StratumContext) checkDisconnect(err error) {
	if err != nil { // actual error
		go sc.Disconnect() // potentially blocking, so async it
	}
}

// Context interface impl

func (StratumContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (StratumContext) Done() <-chan struct{} {
	return nil
}

func (StratumContext) Err() error {
	return nil
}

func (d StratumContext) Value(key any) any {
	return d.parentContext.Value(key)
}
