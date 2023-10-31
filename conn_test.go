package zk

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"sync"
	"testing"
	"time"
)

func TestIntegration_RecurringReAuthHang(t *testing.T) {
	zkC, err := StartTestCluster(t, 3, ioutil.Discard, ioutil.Discard)
	if err != nil {
		panic(err)
	}
	defer zkC.Stop()

	conn, evtC, err := zkC.ConnectAll()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	waitForSession(ctx, evtC)
	// Add auth.
	conn.AddAuth("digest", []byte("test:test"))

	var reauthCloseOnce sync.Once
	reauthSig := make(chan struct{}, 1)
	conn.resendZkAuthFn = func(ctx context.Context, c *Conn) error {
		// in current implimentation the reauth might be called more than once based on various conditions
		reauthCloseOnce.Do(func() { close(reauthSig) })
		return resendZkAuth(ctx, c)
	}

	conn.debugCloseRecvLoop = true
	currentServer := conn.Server()
	zkC.StopServer(currentServer)
	// wait connect to new zookeeper.
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	waitForSession(ctx, evtC)

	select {
	case _, ok := <-reauthSig:
		if !ok {
			return // we closed the channel as expected
		}
		t.Fatal("reauth testing channel should have been closed")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestConcurrentReadAndClose(t *testing.T) {
	WithListenServer(t, func(server string) {
		conn, _, err := Connect([]string{server}, 15*time.Second)
		if err != nil {
			t.Fatalf("Failed to create Connection %s", err)
		}

		okChan := make(chan struct{})
		var setErr error
		go func() {
			_, setErr = conn.Create("/test-path", []byte("test data"), 0, WorldACL(PermAll))
			close(okChan)
		}()

		go func() {
			time.Sleep(1 * time.Second)
			conn.Close()
		}()

		select {
		case <-okChan:
			if setErr != ErrConnectionClosed {
				t.Fatalf("unexpected error returned from Set %v", setErr)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("apparent deadlock!")
		}
	})
}

func TestDeadlockInClose(t *testing.T) {
	c := &Conn{
		shouldQuit:     make(chan struct{}),
		connectTimeout: 1 * time.Second,
		sendChan:       make(chan *request, sendChanSize),
		logger:         DefaultLogger,
	}

	for i := 0; i < sendChanSize; i++ {
		c.sendChan <- &request{}
	}

	okChan := make(chan struct{})
	go func() {
		c.Close()
		close(okChan)
	}()

	select {
	case <-okChan:
	case <-time.After(3 * time.Second):
		t.Fatal("apparent deadlock!")
	}
}

func TestNotifyWatches(t *testing.T) {
	cases := []struct {
		eType   EventType
		path    string
		watches map[watchPathType]bool
	}{
		{
			EventNodeCreated, "/",
			map[watchPathType]bool{
				{"/", watchTypeExist}: true,
				{"/", watchTypeChild}: false,
				{"/", watchTypeData}:  false,
			},
		},
		{
			EventNodeCreated, "/a",
			map[watchPathType]bool{
				{"/b", watchTypeExist}: false,
			},
		},
		{
			EventNodeDataChanged, "/",
			map[watchPathType]bool{
				{"/", watchTypeExist}: true,
				{"/", watchTypeData}:  true,
				{"/", watchTypeChild}: false,
			},
		},
		{
			EventNodeChildrenChanged, "/",
			map[watchPathType]bool{
				{"/", watchTypeExist}: false,
				{"/", watchTypeData}:  false,
				{"/", watchTypeChild}: true,
			},
		},
		{
			EventNodeDeleted, "/",
			map[watchPathType]bool{
				{"/", watchTypeExist}: true,
				{"/", watchTypeData}:  true,
				{"/", watchTypeChild}: true,
			},
		},
	}

	conn := &Conn{watchers: make(map[watchPathType][]chan Event)}

	for idx, c := range cases {
		t.Run(fmt.Sprintf("#%d %s", idx, c.eType), func(t *testing.T) {
			c := c

			notifications := make([]struct {
				path   string
				notify bool
				ch     <-chan Event
			}, len(c.watches))

			var idx int
			for wpt, expectEvent := range c.watches {
				ch := conn.addWatcher(wpt.path, wpt.wType)
				notifications[idx].path = wpt.path
				notifications[idx].notify = expectEvent
				notifications[idx].ch = ch
				idx++
			}
			ev := Event{Type: c.eType, Path: c.path}
			conn.notifyWatches(ev)

			for _, res := range notifications {
				select {
				case e := <-res.ch:
					if !res.notify || e.Path != res.path {
						t.Fatal("unexpeted notification received")
					}
				default:
					if res.notify {
						t.Fatal("expected notification not received")
					}
				}
			}
		})
	}
}

func clientHandler(t *testing.T, ctx context.Context, conn net.Conn) {
	defer conn.Close()

	expiredSession := int64(123456)
	newSession := int64(654321)
	_ = newSession

	nBuf := make([]byte, 4)
	_, err := io.ReadFull(conn, nBuf)
	if err != nil {
		t.Errorf("Read connect request length failed, %v", err)
		return
	}

	buf := make([]byte, binary.BigEndian.Uint32(nBuf))
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		t.Errorf("Read connect request failed, %v", err)
		return
	}

	var req connectRequest
	_, err = decodePacket(buf, &req)
	if err != nil {
		t.Errorf("Decode connect request failed, %v", err)
		return
	}
	log.Println("Receive connect request")

	buf = make([]byte, 256)
	var rsp connectResponse
	rsp.ProtocolVersion = 0
	if req.SessionID == expiredSession {
		rsp.SessionID = 0
		rsp.TimeOut = 0
		rsp.Passwd = make([]byte, 16)
	} else {
		return
		//rsp.SessionID = newSession
		//rsp.TimeOut = 2000
		//rsp.Passwd = []byte("deadbeefdeadbeef")
	}

	n, err := encodePacket(buf[4:], &rsp)
	if err != nil {
		t.Errorf("Encode connect response failed, %v", err)
		return
	}
	binary.BigEndian.PutUint32(buf[:4], uint32(n))

	_, err = conn.Write(buf[:4])
	if err != nil {
		t.Errorf("Send connect response length failed, %v", err)
		return
	}

	_, err = conn.Write(buf[4:])
	if err != nil {
		t.Errorf("Send connect response failed, %v", err)
		return
	}
	log.Println("Connect response sent")

	// handle ping request
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		nBuf = make([]byte, 4)
		_, err = conn.Read(nBuf)
		if err != nil {
			t.Errorf("Read ping request length failed, %v", err)
			return
		}

		buf = make([]byte, binary.BigEndian.Uint32(nBuf))
		_, err = io.ReadFull(conn, buf)
		if err != nil {
			t.Errorf("Read ping request failed, %v", err)
			return
		}

		rsp := make([]byte, 32)
		n, err := encodePacket(rsp[4:], &responseHeader{Xid: -2, Zxid: 0x1, Err: 0})
		if err != nil {
			t.Errorf("Encode ping response failed, %v", err)
			return
		}
		binary.BigEndian.PutUint32(buf[:4], uint32(n))

		_, err = conn.Write(rsp)
		if err != nil {
			t.Errorf("Send ping response failed, %v", err)
		}
	}
}

func TestSessionExpired(t *testing.T) {
	// run mock server
	a := make(chan net.Addr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func(ctx context.Context) {
		ln, err := net.Listen("tcp", ":0")
		if err != nil {
			t.Errorf("listen failed, %v", err)
			return
		}
		a <- ln.Addr()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			conn, err := ln.Accept()
			if err != nil {
				t.Errorf("accept failed, %v", err)
				return
			}
			log.Printf("Accept client: %v", conn.RemoteAddr())

			go clientHandler(t, ctx, conn)
		}
	}(ctx)

	serverSock := <-a

	c := &Conn{
		dialer: func(network, address string, timeout time.Duration) (net.Conn, error) {
			return net.Dial(serverSock.Network(), serverSock.String())
		},
		shouldQuit:     make(chan struct{}),
		connectTimeout: time.Second,
		recvTimeout:    2 * time.Second,
		sendChan:       make(chan *request, sendChanSize),
		logger:         DefaultLogger,
		hostProvider:   &EmptyHostProvider{},
		sessionID:      123456,
		resendZkAuthFn: resendZkAuth,
		buf:            make([]byte, bufferSize),
	}
	_ = c.hostProvider.Init([]string{"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"})

	c.loop(context.Background())
}

type EmptyHostProvider struct {
	idx     int
	servers []string
}

func (e *EmptyHostProvider) Init(servers []string) error {
	e.idx = 0
	e.servers = servers
	return nil
}

func (e *EmptyHostProvider) Len() int {
	return len(e.servers)
}

func (e *EmptyHostProvider) Connected() {
	log.Printf("Connected to %v", e.idx)
}

func (e *EmptyHostProvider) Next() (server string, retryStart bool) {
	if e.idx >= len(e.servers) {
		retryStart = true
		e.idx = 0
	}

	server = e.servers[e.idx]
	e.idx++

	log.Printf("Next server is %v", server)
	return
}
