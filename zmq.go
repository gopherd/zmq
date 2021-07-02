package zmq

import (
	"context"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	zmq "github.com/pebbe/zmq4"

	"github.com/gopherd/doge/mq"
	"github.com/gopherd/doge/query"
	"github.com/gopherd/doge/service/discovery"
)

func init() {
	mq.Register("zmq", new(driver))
}

type driver struct {
}

// source format:
//
// [tcp://]host:port
//
func (d driver) Open(source string, discovery discovery.Discovery) (mq.Conn, error) {
	var opt options
	if err := opt.parse(source); err != nil {
		return nil, err
	}
	return newConn(opt, discovery), nil
}

type options struct {
	scheme string
	host   string
}

func (opt options) addr() string {
	if opt.scheme == "" {
		return opt.host
	}
	return opt.scheme + "://" + opt.host
}

func (opt *options) parse(source string) error {
	u, err := query.ParseURL(source, "tcp")
	if err != nil {
		return err
	}
	opt.scheme = u.Scheme
	opt.host = u.Host
	return nil
}

// conn is the top-level zmq connection
type conn struct {
	options   options
	discovery discovery.Discovery

	pushersMu sync.RWMutex
	pushers   map[string]*pusher

	pullersMu sync.Mutex
	pullers   map[string]*puller
}

func newConn(opt options, discovery discovery.Discovery) *conn {
	return &conn{
		options:   opt,
		discovery: discovery,
		pullers:   make(map[string]*puller),
		pushers:   make(map[string]*pusher),
	}
}

// Close closes the conn
func (c *conn) Close() error {
	c.pullersMu.Lock()
	for topic, puller := range c.pullers {
		puller.shutdown()
		delete(c.pullers, topic)
	}
	c.pullersMu.Unlock()

	c.pushersMu.Lock()
	for topic, pusher := range c.pushers {
		pusher.shutdown()
		delete(c.pushers, topic)
	}
	c.pushersMu.Unlock()

	return nil
}

// Ping implements mq.Conn Ping method
func (c *conn) Ping(topic string) error {
	_, err := c.getPusher(topic)
	return err
}

// Subscribe implements mq.Conn Subscribe method
func (c *conn) Subscribe(topic string, consumer mq.Consumer) error {
	c.pullersMu.Lock()
	defer c.pullersMu.Unlock()

	if _, ok := c.pullers[topic]; ok {
		return nil
	}
	addr := c.options.addr()
	p, err := newPuller(topic, addr, consumer)
	if err != nil {
		return err
	}
	if err := c.discovery.Register(context.TODO(), topic, "0", addr, false, 0); err != nil {
		return err
	}
	go p.start()
	c.pullers[topic] = p
	return nil
}

// Publish implements mq.Conn Publish method
func (c *conn) Publish(topic string, content []byte) error {
	p, err := c.getPusher(topic)
	if err != nil {
		return err
	}
	return p.publish(content)
}

func (c *conn) getPusher(topic string) (*pusher, error) {
	c.pushersMu.RLock()
	p, ok := c.pushers[topic]
	if ok {
		c.pushersMu.RUnlock()
		return p, nil
	}
	content, err := c.discovery.Find(context.TODO(), topic, "0")
	if err != nil {
		return nil, err
	}
	p, err = newPusher(content)
	if err == nil {
		c.pushersMu.Lock()
		defer c.pushersMu.Unlock()
		if p2, ok := c.pushers[topic]; ok {
			p.shutdown()
			return p2, nil
		}
		c.pushers[topic] = p
	}
	return p, err
}

// pusher used publish messages
type pusher struct {
	socket *zmq.Socket
}

func newPusher(addr string) (*pusher, error) {
	socket, err := zmq.NewSocket(zmq.PUSH)
	if err == nil {
		err = socket.Connect(addr)
	}
	if err != nil {
		return nil, err
	}
	return &pusher{
		socket: socket,
	}, nil
}

func (p *pusher) publish(data []byte) error {
	_, err := p.socket.SendBytes(data, zmq.DONTWAIT)
	return err
}

func (p *pusher) shutdown() {
	p.socket.Close()
}

// puller used to receive messages from specified topic
type puller struct {
	running  int32
	topic    string
	socket   *zmq.Socket
	consumer mq.Consumer
	claim    *claim
}

func newPuller(topic, addr string, consumer mq.Consumer) (*puller, error) {
	socket, err := zmq.NewSocket(zmq.PULL)
	if err == nil {
		err = socket.Bind(addr)
	}
	if err != nil {
		return nil, err
	}
	p := &puller{
		topic:    topic,
		socket:   socket,
		consumer: consumer,
		claim:    newClaim(),
	}
	if err := p.consumer.Setup(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *puller) start() error {
	atomic.StoreInt32(&p.running, 1)
	go p.consumer.Consume(p.topic, p.claim)

	const interval = time.Millisecond
	var timer *time.Timer
	for {
		data, err := p.pull()
		if err != nil {
			if atomic.LoadInt32(&p.running) == 0 {
				p.claim.err <- nil
			} else {
				p.claim.err <- err
			}
			break
		}
		if data != nil {
			p.claim.msg <- data
		} else {
			if timer == nil {
				timer = time.NewTimer(interval)
			} else {
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(interval)
			}
			<-timer.C
		}
	}
	if timer != nil {
		timer.Stop()
	}
	return p.consumer.Cleanup()
}

func (p *puller) pull() ([]byte, error) {
	data, err := p.socket.RecvBytes(zmq.DONTWAIT)
	if err != nil {
		errno := zmq.AsErrno(err)
		switch errno {
		case zmq.Errno(syscall.EAGAIN):
		default:
			return nil, err
		}
		return nil, nil
	}
	return data, nil
}

func (p *puller) shutdown() {
	atomic.StoreInt32(&p.running, 0)
	p.socket.Close()
}

// claim implements mq.Claim
type claim struct {
	err chan error
	msg chan []byte
}

func newClaim() *claim {
	return &claim{
		err: make(chan error),
		msg: make(chan []byte, 64),
	}
}

func (claim *claim) Err() <-chan error      { return claim.err }
func (claim *claim) Message() <-chan []byte { return claim.msg }
