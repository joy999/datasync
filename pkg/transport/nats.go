package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	datasync "github.com/joy999/datasync/pkg"
	"github.com/nats-io/nats.go"
)

// NATSConfig 表示 NATS 传输层配置。
type NATSConfig struct {
	Servers           []string
	Username          string
	Password          string
	Token             string
	TLSCert           string
	TLSKey            string
	TLSCA             string
	ReconnectWait     time.Duration
	MaxReconnects     int
	ConnectionTimeout time.Duration
	EnableJetStream   bool
	StreamName        string
}

// NodeInfo 表示发现到的节点信息。
type NodeInfo struct {
	NodeID    string    `json:"node_id"`
	GroupID   string    `json:"group_id"`
	Address   string    `json:"address"`
	IsLeader  bool      `json:"is_leader"`
	Timestamp time.Time `json:"timestamp"`
}

// ErrorHandler 定义传输层错误处理函数。
type ErrorHandler func(ctx context.Context, err error)

// NATS 是基于 NATS 的传输层实现。
type NATS struct {
	config *NATSConfig
	conn   *nats.Conn
	js     nats.JetStreamContext
	ctx    context.Context
	cancel context.CancelFunc

	subs         map[string]*nats.Subscription
	subCallbacks map[string][]func([]byte)
	subsMutex    sync.RWMutex

	knownNodes        map[datasync.NodeID]*NodeInfo
	nodesMutex        sync.RWMutex
	discoverySub      *nats.Subscription
	discoveryInterval time.Duration
	localNode         NodeInfo
	localNodeMutex    sync.RWMutex

	errorHandler ErrorHandler
}

func (c *NATSConfig) setDefaults() {
	if c.ReconnectWait == 0 {
		c.ReconnectWait = time.Second
	}
	if c.MaxReconnects == 0 {
		c.MaxReconnects = 60
	}
	if c.ConnectionTimeout == 0 {
		c.ConnectionTimeout = 10 * time.Second
	}
	if c.StreamName == "" {
		c.StreamName = "RAFTSYNC"
	}
}

func (c *NATSConfig) buildOptions() []nats.Option {
	opts := []nats.Option{
		nats.ReconnectWait(c.ReconnectWait),
		nats.MaxReconnects(c.MaxReconnects),
		nats.Timeout(c.ConnectionTimeout),
	}

	if c.Username != "" && c.Password != "" {
		opts = append(opts, nats.UserInfo(c.Username, c.Password))
	} else if c.Token != "" {
		opts = append(opts, nats.Token(c.Token))
	}

	if c.TLSCert != "" && c.TLSKey != "" {
		opts = append(opts, nats.ClientCert(c.TLSCert, c.TLSKey))
	}
	if c.TLSCA != "" {
		opts = append(opts, nats.RootCAs(c.TLSCA))
	}

	return opts
}

// NewNATS 创建一个新的 NATS 传输层实例。
func NewNATS(config *NATSConfig) (*NATS, error) {
	ctx, cancel := context.WithCancel(context.Background())
	config.setDefaults()

	if len(config.Servers) == 0 {
		cancel()
		return nil, fmt.Errorf("no NATS servers configured")
	}

	conn, err := nats.Connect(config.Servers[0], config.buildOptions()...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	n := &NATS{
		config:            config,
		conn:              conn,
		ctx:               ctx,
		cancel:            cancel,
		subs:              make(map[string]*nats.Subscription),
		subCallbacks:      make(map[string][]func([]byte)),
		knownNodes:        make(map[datasync.NodeID]*NodeInfo),
		discoveryInterval: 30 * time.Second,
	}

	if err := n.setupJetStream(); err != nil {
		conn.Close()
		cancel()
		return nil, err
	}
	if err := n.startDiscovery(); err != nil {
		conn.Close()
		cancel()
		return nil, fmt.Errorf("failed to start discovery: %w", err)
	}

	return n, nil
}

func (n *NATS) setupJetStream() error {
	if !n.config.EnableJetStream {
		return nil
	}

	js, err := n.conn.JetStream()
	if err != nil {
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}
	n.js = js

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     n.config.StreamName,
		Subjects: []string{"raftsync.*"},
	})
	if err != nil {
		if _, infoErr := js.StreamInfo(n.config.StreamName); infoErr != nil {
			return fmt.Errorf("failed to create or get stream: %w", err)
		}
	}

	return nil
}

// Start 检查并确认传输连接可用。
func (n *NATS) Start(ctx context.Context) error {
	if !n.conn.IsConnected() {
		return fmt.Errorf("NATS connection is not established")
	}

	return nil
}

// Stop 关闭订阅并断开 NATS 连接。
func (n *NATS) Stop() error {
	var stopErrs []error

	n.cancel()

	n.subsMutex.RLock()
	subs := make([]*nats.Subscription, 0, len(n.subs))
	for _, sub := range n.subs {
		subs = append(subs, sub)
	}
	n.subsMutex.RUnlock()

	for _, sub := range subs {
		if err := sub.Unsubscribe(); err != nil {
			stopErrs = append(stopErrs, fmt.Errorf("failed to unsubscribe: %w", err))
		}
	}

	if n.discoverySub != nil {
		if err := n.discoverySub.Unsubscribe(); err != nil {
			stopErrs = append(stopErrs, fmt.Errorf("failed to unsubscribe discovery: %w", err))
		}
	}

	n.conn.Close()

	if len(stopErrs) > 0 {
		return fmt.Errorf("stop errors: %v", stopErrs)
	}

	return nil
}

// SendMessage 向目标组主题发布消息。
func (n *NATS) SendMessage(ctx context.Context, targetGroup datasync.GroupID, message []byte) error {
	subject := fmt.Sprintf("raftsync.%s", targetGroup)

	if n.config.EnableJetStream && n.js != nil {
		if _, err := n.js.Publish(subject, message); err != nil {
			return fmt.Errorf("failed to publish message to JetStream: %w", err)
		}
		return nil
	}

	if err := n.conn.Publish(subject, message); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// Subscribe 为指定组注册消息回调。
func (n *NATS) Subscribe(ctx context.Context, groupID datasync.GroupID, callback func([]byte)) error {
	subject := fmt.Sprintf("raftsync.%s", groupID)

	n.subsMutex.Lock()
	defer n.subsMutex.Unlock()

	n.subCallbacks[subject] = append(n.subCallbacks[subject], callback)
	if _, ok := n.subs[subject]; ok {
		return nil
	}

	dispatch := func(data []byte) {
		n.subsMutex.RLock()
		callbacks := append([]func([]byte){}, n.subCallbacks[subject]...)
		n.subsMutex.RUnlock()

		for _, cb := range callbacks {
			cb(data)
		}
	}

	var (
		sub *nats.Subscription
		err error
	)

	if n.config.EnableJetStream && n.js != nil {
		sub, err = n.js.Subscribe(subject, func(msg *nats.Msg) {
			dispatch(msg.Data)
			if ackErr := msg.Ack(); ackErr != nil && n.errorHandler != nil {
				n.errorHandler(n.ctx, fmt.Errorf("failed to ack message: %w", ackErr))
			}
		}, nats.Durable(fmt.Sprintf("durable-%s", groupID)))
	} else {
		sub, err = n.conn.Subscribe(subject, func(msg *nats.Msg) {
			dispatch(msg.Data)
		})
	}
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	n.subs[subject] = sub
	return nil
}

func (n *NATS) startDiscovery() error {
	sub, err := n.conn.Subscribe("raftsync.discovery", func(msg *nats.Msg) {
		n.handleDiscoveryMessage(msg.Data)
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to discovery: %w", err)
	}

	n.discoverySub = sub
	go n.discoveryLoop()
	return nil
}

func (n *NATS) discoveryLoop() {
	ticker := time.NewTicker(n.discoveryInterval)
	defer ticker.Stop()

	n.broadcastNodeInfo()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.broadcastNodeInfo()
		}
	}
}

func (n *NATS) broadcastNodeInfo() {
	n.localNodeMutex.RLock()
	info := n.localNode
	n.localNodeMutex.RUnlock()
	if info.NodeID == "" || info.GroupID == "" {
		return
	}

	info.Timestamp = time.Now()
	if info.Address == "" {
		info.Address = n.conn.ConnectedAddr()
	}

	data, err := json.Marshal(&info)
	if err != nil {
		return
	}

	if err := n.conn.Publish("raftsync.discovery", data); err != nil && n.errorHandler != nil {
		n.errorHandler(n.ctx, fmt.Errorf("failed to publish discovery message: %w", err))
	}
}

func (n *NATS) handleDiscoveryMessage(data []byte) {
	var info NodeInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return
	}
	if info.NodeID == "" || info.GroupID == "" {
		return
	}

	n.nodesMutex.Lock()
	defer n.nodesMutex.Unlock()

	existing, ok := n.knownNodes[datasync.NodeID(info.NodeID)]
	if ok {
		existing.GroupID = info.GroupID
		existing.Address = info.Address
		existing.IsLeader = info.IsLeader
		existing.Timestamp = info.Timestamp
		return
	}

	infoCopy := info
	n.knownNodes[datasync.NodeID(info.NodeID)] = &infoCopy
}

// DiscoverNodes 返回指定组中仍然有效的节点列表。
func (n *NATS) DiscoverNodes(ctx context.Context, groupID datasync.GroupID) ([]datasync.NodeID, error) {
	n.nodesMutex.RLock()
	defer n.nodesMutex.RUnlock()

	nodes := make([]datasync.NodeID, 0, len(n.knownNodes))
	for nodeID, info := range n.knownNodes {
		if info.GroupID != string(groupID) {
			continue
		}
		if time.Since(info.Timestamp) >= 5*time.Minute {
			continue
		}
		nodes = append(nodes, nodeID)
	}

	return nodes, nil
}

// GetKnownNodes 返回已发现节点的副本。
func (n *NATS) GetKnownNodes() map[datasync.NodeID]*NodeInfo {
	n.nodesMutex.RLock()
	defer n.nodesMutex.RUnlock()

	result := make(map[datasync.NodeID]*NodeInfo, len(n.knownNodes))
	for nodeID, info := range n.knownNodes {
		infoCopy := *info
		result[nodeID] = &infoCopy
	}

	return result
}

// RemoveStaleNodes 删除超过最大存活时间的节点。
func (n *NATS) RemoveStaleNodes(maxAge time.Duration) int {
	n.nodesMutex.Lock()
	defer n.nodesMutex.Unlock()

	removed := 0
	now := time.Now()
	for nodeID, info := range n.knownNodes {
		if now.Sub(info.Timestamp) > maxAge {
			delete(n.knownNodes, nodeID)
			removed++
		}
	}

	return removed
}

// SetErrorHandler 设置传输层错误处理器。
func (n *NATS) SetErrorHandler(handler ErrorHandler) {
	n.errorHandler = handler
}

// SetLocalNodeInfo 设置通过发现机制广播的本地节点身份。
func (n *NATS) SetLocalNodeInfo(nodeID datasync.NodeID, groupID datasync.GroupID, address string) {
	n.localNodeMutex.Lock()
	defer n.localNodeMutex.Unlock()

	n.localNode = NodeInfo{
		NodeID:    string(nodeID),
		GroupID:   string(groupID),
		Address:   address,
		Timestamp: time.Now(),
	}
}

// SetLeaderState 更新通过发现机制发布的 Leader 状态。
func (n *NATS) SetLeaderState(isLeader bool) {
	n.localNodeMutex.Lock()
	defer n.localNodeMutex.Unlock()

	n.localNode.IsLeader = isLeader
	n.localNode.Timestamp = time.Now()
}
