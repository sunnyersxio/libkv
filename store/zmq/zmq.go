package zmq

import (
	"errors"
	"github.com/json-iterator/go"
	zmq "github.com/pebbe/zmq3"
	"github.com/sunnyersxio/libkv"
	"github.com/sunnyersxio/libkv/store"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultWatchWaitTime is how long we block for at a
	// time to check if the watched key has changed. This
	// affects the minimum time it takes to cancel a watch.
	DefaultWatchWaitTime = 15 * time.Second
	
	// RenewSessionRetryMax is the number of time we should try
	// to renew the session before giving up and throwing an error
	RenewSessionRetryMax = 5
	
	// MaxSessionDestroyAttempts is the maximum times we will try
	// to explicitely destroy the session attached to a lock after
	// the connectivity to the store has been lost
	MaxSessionDestroyAttempts = 5
	
	// defaultLockTTL is the default ttl for the consul lock
	defaultLockTTL = 20 * time.Second
)

var (
	// ErrMultipleEndpointsUnsupported is thrown when there are
	// multiple endpoints specified for Consul
	ErrMultipleEndpointsUnsupported = errors.New("consul does not support multiple endpoints")
	
	// ErrSessionRenew is thrown when the session can't be
	// renewed because the Consul version does not support sessions
	ErrSessionRenew = errors.New("cannot set or renew session for ttl, unable to operate on sessions")
)

// Consul is the receiver type for the
// Store interface
type Zmq struct {
	sync.Mutex
	conn []*zmq.Socket
}

// Register registers consul to libkv
func Register() {
	libkv.AddStore(store.ZMQ, New)
}

// New creates a new Consul client given a list
// of endpoints and optional tls config
func New(endpoints []string, options *store.Config) (store.Store, error) {
	if len(endpoints) == 0 {
		return nil, ErrMultipleEndpointsUnsupported
	}
	var err error
	s := &Zmq{}
	for _, v := range endpoints {
		conn, err := zmq.NewSocket(zmq.REQ)
		if err != nil {
			continue
		}
		err = conn.SetReconnectIvl(10000000)
		if err != nil {
			continue
		}
		err = conn.Connect(v)
		if err != nil {
			continue
		}
		s.conn = append(s.conn, conn)
	}
	
	if len(s.conn) == 0 {
		return nil, err
	}
	return s, nil
}

// Get the value at "key", returns the last modified index
// to use in conjunction to CAS calls
func (s *Zmq) Get(key string) (*store.KVPair, error) {
	filter := make(map[string]bool)
	res := make([]string, 0)
	for _, conn := range s.conn {
		_, err := conn.SendMessage(key)
		if err != nil {
			continue
		}
		msg, err := conn.Recv(0)
		if err != nil {
			continue
		}
		ipList := strings.Split(msg,"|")
		for _,ip := range ipList[1:] {
			if _, ok := filter[ip]; !ok {
				res = append(res, ip)
				filter[ip] = true
			}
		}
	}
	js, _ := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(res)
	return &store.KVPair{Key: key, Value: js, LastIndex: uint64(time.Now().Unix())}, nil
}

func (s *Zmq) Put(key string, value []byte, options *store.WriteOptions) error {
	for _, conn := range s.conn {
		_, err := conn.SendMessage(key)
		if err != nil {
			continue
		}
		_, err = conn.RecvMessageBytes(0)
		if err != nil {
			continue
		}
	}
	return nil
}

// Delete the value at the specified key
func (s *Zmq) Delete(key string) error {
	return nil
}

// Verify if a Key exists in the store
func (s *Zmq) Exists(key string) (bool, error) {
	return true, nil
}

// Watch for changes on a key
func (s *Zmq) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	watchCh := make(chan *store.KVPair)
	go func() {
		defer close(watchCh)
		ticker := time.NewTicker(time.Second * 3)
		
		
		// Use a wait time in order to check if we should quit
		// from time to time.
		for {
			// Check if we should quit
			select {
			case <-stopCh:
				return
			case <- ticker.C:
				// Get the key
				pair, err := s.Get(key)
				if err != nil {
					return
				}
				// If LastIndex didn't change then it means `Get` returned
				// because of the WaitTime and the key didn't changed.
				// Return the value to the channel
				// FIXME: What happens when a key is deleted?
				if pair != nil {
					watchCh <- &store.KVPair{
						Key:       pair.Key,
						Value:     pair.Value,
						LastIndex: uint64(time.Now().Unix()),
					}
				}
			default:
			}
		}
	}()
	
	return watchCh, nil
}

// WatchTree watches for changes on child nodes under
// a given directory
func (s *Zmq) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	watchCh := make(chan []*store.KVPair)
	go func() {
		defer close(watchCh)
		ticker := time.NewTicker(time.Second * 3)
		// Use a wait time in order to check if we should quit
		// from time to time.
		for {
			// Check if we should quit
			select {
			case <-stopCh:
				return
			case <- ticker.C:
				// Get all the childrens
				pairs, err := s.List(directory)
				if err != nil {
					return
				}
				// Return children KV pairs to the channel
				kvpairs := make([]*store.KVPair,0)
				for _, pair := range pairs {
					kvpairs = append(kvpairs, &store.KVPair{
						Key:       pair.Key,
						Value:     pair.Value,
						LastIndex: uint64(time.Now().Unix()),
					})
				}
				watchCh <- kvpairs
			default:
			}
		}
	}()
	
	return watchCh, nil
}

// NewLock creates a lock for a given key.
// The returned Locker is not held and must be acquired
// with `.Lock`. The Value is optional.
func (s *Zmq) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, nil
}

// List the content of a given prefix
func (s *Zmq) List(directory string) ([]*store.KVPair, error) {
	filter := make(map[string]bool)
	res := make([]*store.KVPair, 0)
	for _, conn := range s.conn {
		_, err := conn.SendMessage(directory)
		if err != nil {
			continue
		}
		msg, err := conn.Recv(0)
		if err != nil {
			continue
		}
		ipList := strings.Split(msg,"|")
		for _,ip := range ipList[1:] {
			if _, ok := filter[ip]; !ok {
				res = append(res, &store.KVPair{Key: directory, Value: []byte(ip), LastIndex: uint64(time.Now().Unix())})
				filter[ip] = true
			}
		}
	}
	return res, nil
}

// DeleteTree deletes a range of keys under a given directory
func (s *Zmq) DeleteTree(directory string) error {
	return nil
}

// Atomic CAS operation on a single value.
// Pass previous = nil to create a new key.
func (s *Zmq) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	return true, nil, nil
}

// Atomic delete of a single value
func (s *Zmq) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	return true, nil
}

// Close the store connection
func (s *Zmq) Close() {
	for _, conn := range s.conn {
		conn.Close()
	}
}
