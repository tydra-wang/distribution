package notifications

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/distribution/distribution/v3/configuration"
	events "github.com/docker/go-events"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type etcdMQSink struct {
	timeout time.Duration
	client  *clientv3.Client
	conf    configuration.EtcdMQ

	closed bool
	mu     sync.Mutex
}

func NewEtcdMQSink(conf configuration.EtcdMQ) events.Sink {
	var sink events.Sink
	sink = &etcdMQSink{
		timeout: time.Second * 5,
		conf:    conf,
	}

	sink = events.NewRetryingSink(sink, events.NewBreaker(3, time.Second*3))
	sink = newEventQueue(sink)
	sink = newIgnoredSink(sink, []string{"application/octet-stream"}, []string{EventActionPull, EventActionMount})
	return sink
}

func initEtcdClient(conf configuration.EtcdMQ) (*clientv3.Client, error) {
	config := clientv3.Config{
		Endpoints:        conf.Endpoints,
		AutoSyncInterval: time.Hour,
	}
	if conf.Cafile != "" {
		tlsConfig, err := (transport.TLSInfo{
			TrustedCAFile: conf.Cafile,
			CertFile:      conf.Certfile,
			KeyFile:       conf.Keyfile,
		}).ClientConfig()
		if err != nil {
			return nil, err
		}
		config.TLS = tlsConfig
	}
	return clientv3.New(config)
}

// Write an event to the Sink. If no error is returned, the caller will
// assume that all events have been committed to the sink. If an error is
// received, the caller may retry sending the event.
func (s *etcdMQSink) Write(event events.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSinkClosed
	}
	if s.client == nil {
		client, err := initEtcdClient(s.conf)
		if err != nil {
			return err
		}
		s.client = client
	}
	e := event.(Event)
	if strings.Contains(e.Request.UserAgent, "docker-registry-synchronizer") {
		return nil
	}
	data, _ := json.MarshalIndent(e, "\t", "\t")
	fmt.Printf("etcd mq writing event: %s", string(data))
	if e.Target.Tag == "" {
		return nil
	}
	key := path.Join(s.conf.Prefix, e.Target.Repository) + ":" + e.Target.Tag
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	_, err := s.client.Put(ctx, key, "")
	return err
}

// Close the sink, possibly waiting for pending events to flush.
func (s *etcdMQSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("httpsink: already closed")
	}

	s.closed = true
	return nil
}
