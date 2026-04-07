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

// NATSConfig NATS配置
type NATSConfig struct {
	Servers           []string      // NATS服务器地址
	Username          string        // 用户名
	Password          string        // 密码
	Token             string        // 令牌
	TLSCert           string        // TLS证书
	TLSKey            string        // TLS密钥
	TLSCA             string        // TLS CA
	ReconnectWait     time.Duration // 重连等待时间
	MaxReconnects     int           // 最大重连次数
	ConnectionTimeout time.Duration // 连接超时
	EnableJetStream   bool          // 是否启用JetStream
	StreamName        string        // JetStream流名称
}

// NodeInfo 节点信息
type NodeInfo struct {
	NodeID    string    `json:"node_id"`
	GroupID   string    `json:"group_id"`
	Address   string    `json:"address"`
	IsLeader  bool      `json:"is_leader"`
	Timestamp time.Time `json:"timestamp"`
}

// ErrorHandler 错误处理函数类型
type ErrorHandler func(ctx context.Context, err error)

// NATS NATS传输层实现
type NATS struct {
	config *NATSConfig
	conn   *nats.Conn
	js     nats.JetStreamContext
	subs   map[string]*nats.Subscription
	ctx    context.Context
	cancel context.CancelFunc

	// 节点发现相关
	knownNodes        map[datasync.NodeID]*NodeInfo
	nodesMutex        sync.RWMutex
	discoverySub      *nats.Subscription
	discoveryInterval time.Duration

	// 错误处理器
	errorHandler ErrorHandler
}

// setDefaults 设置默认配置值
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

// buildOptions 构建NATS连接选项
func (c *NATSConfig) buildOptions() []nats.Option {
	opts := []nats.Option{
		nats.ReconnectWait(c.ReconnectWait),
		nats.MaxReconnects(c.MaxReconnects),
		nats.Timeout(c.ConnectionTimeout),
	}

	// 添加认证选项
	if c.Username != "" && c.Password != "" {
		opts = append(opts, nats.UserInfo(c.Username, c.Password))
	} else if c.Token != "" {
		opts = append(opts, nats.Token(c.Token))
	}

	// 添加TLS选项
	if c.TLSCert != "" && c.TLSKey != "" {
		opts = append(opts, nats.ClientCert(c.TLSCert, c.TLSKey))
	}

	if c.TLSCA != "" {
		opts = append(opts, nats.RootCAs(c.TLSCA))
	}

	return opts
}

// NewNATS 创建NATS传输层
func NewNATS(config *NATSConfig) (*NATS, error) {
	// 初始化上下文
	ctx, cancel := context.WithCancel(context.Background())

	// 设置默认值
	config.setDefaults()

	// 检查服务器配置
	if len(config.Servers) == 0 {
		cancel()
		return nil, fmt.Errorf("no NATS servers configured")
	}

	// 连接到NATS
	conn, err := nats.Connect(config.Servers[0], config.buildOptions()...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// 初始化NATS
	n := &NATS{
		config:            config,
		conn:              conn,
		subs:              make(map[string]*nats.Subscription),
		ctx:               ctx,
		cancel:            cancel,
		knownNodes:        make(map[datasync.NodeID]*NodeInfo),
		discoveryInterval: 30 * time.Second,
	}

	// 启用JetStream
	if err := n.setupJetStream(); err != nil {
		conn.Close()
		cancel()
		return nil, err
	}

	// 启动节点发现
	if err := n.startDiscovery(); err != nil {
		conn.Close()
		cancel()
		return nil, fmt.Errorf("failed to start discovery: %w", err)
	}

	return n, nil
}

// setupJetStream 设置JetStream
func (n *NATS) setupJetStream() error {
	if !n.config.EnableJetStream {
		return nil
	}

	js, err := n.conn.JetStream()
	if err != nil {
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}
	n.js = js

	// 创建流（如果不存在）
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     n.config.StreamName,
		Subjects: []string{"raftsync.*"},
	})
	if err != nil {
		// 流可能已存在，尝试获取
		_, err = js.StreamInfo(n.config.StreamName)
		if err != nil {
			return fmt.Errorf("failed to create or get stream: %w", err)
		}
	}

	return nil
}

// Start 启动传输层
func (n *NATS) Start(ctx context.Context) error {
	// 检查连接状态
	if !n.conn.IsConnected() {
		return fmt.Errorf("NATS connection is not established")
	}

	return nil
}

// Stop 停止传输层
func (n *NATS) Stop() error {
	var stopErrs []error

	// 取消上下文
	n.cancel()

	// 取消所有订阅
	for _, sub := range n.subs {
		if err := sub.Unsubscribe(); err != nil {
			stopErrs = append(stopErrs, fmt.Errorf("failed to unsubscribe: %w", err))
		}
	}

	// 取消发现订阅
	if n.discoverySub != nil {
		if err := n.discoverySub.Unsubscribe(); err != nil {
			stopErrs = append(stopErrs, fmt.Errorf("failed to unsubscribe discovery: %w", err))
		}
	}

	// 关闭连接
	n.conn.Close()

	if len(stopErrs) > 0 {
		return fmt.Errorf("stop errors: %v", stopErrs)
	}
	return nil
}

// SendMessage 发送消息
func (n *NATS) SendMessage(ctx context.Context, targetGroup datasync.GroupID, message []byte) error {
	subject := fmt.Sprintf("raftsync.%s", targetGroup)

	if n.config.EnableJetStream && n.js != nil {
		// 使用JetStream发送消息
		_, err := n.js.Publish(subject, message)
		if err != nil {
			return fmt.Errorf("failed to publish message to JetStream: %w", err)
		}
	} else {
		// 直接发送消息
		if err := n.conn.Publish(subject, message); err != nil {
			return fmt.Errorf("failed to publish message: %w", err)
		}
	}

	return nil
}

// Subscribe 订阅消息
func (n *NATS) Subscribe(ctx context.Context, groupID datasync.GroupID, callback func([]byte)) error {
	subject := fmt.Sprintf("raftsync.%s", groupID)

	// 取消旧的订阅
	if sub, ok := n.subs[subject]; ok {
		if err := sub.Unsubscribe(); err != nil {
			// 记录错误但继续创建新订阅
			if n.errorHandler != nil {
				n.errorHandler(n.ctx, fmt.Errorf("failed to unsubscribe: %w", err))
			}
		}
	}

	// 创建新的订阅
	var sub *nats.Subscription
	var err error

	if n.config.EnableJetStream && n.js != nil {
		// 使用 JetStream 订阅
		sub, err = n.js.Subscribe(subject, func(msg *nats.Msg) {
			callback(msg.Data)
			if ackErr := msg.Ack(); ackErr != nil {
				// Ack 失败，记录错误
				if n.errorHandler != nil {
					n.errorHandler(n.ctx, fmt.Errorf("failed to ack message: %w", ackErr))
				}
			}
		}, nats.Durable(fmt.Sprintf("durable-%s", groupID)))
	} else {
		// 普通订阅
		sub, err = n.conn.Subscribe(subject, func(msg *nats.Msg) {
			callback(msg.Data)
		})
	}

	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	n.subs[subject] = sub
	return nil
}

// startDiscovery 启动节点发现
func (n *NATS) startDiscovery() error {
	// 订阅节点发现主题
	subject := "raftsync.discovery"

	sub, err := n.conn.Subscribe(subject, func(msg *nats.Msg) {
		n.handleDiscoveryMessage(msg.Data)
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to discovery: %w", err)
	}
	n.discoverySub = sub

	// 启动定期广播协程
	go n.discoveryLoop()

	return nil
}

// discoveryLoop 定期广播节点信息
func (n *NATS) discoveryLoop() {
	ticker := time.NewTicker(n.discoveryInterval)
	defer ticker.Stop()

	// 立即广播一次
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

// broadcastNodeInfo 广播节点信息
func (n *NATS) broadcastNodeInfo() {
	// 这里简化处理，实际应该从节点获取信息
	info := &NodeInfo{
		NodeID:    "", // 需要在初始化时设置
		GroupID:   "", // 需要在初始化时设置
		Address:   n.conn.ConnectedAddr(),
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(info)
	if err != nil {
		return
	}

	if err := n.conn.Publish("raftsync.discovery", data); err != nil {
		// 发布失败，记录错误
		if n.errorHandler != nil {
			n.errorHandler(n.ctx, fmt.Errorf("failed to publish discovery message: %w", err))
		}
	}
}

// handleDiscoveryMessage 处理发现消息
func (n *NATS) handleDiscoveryMessage(data []byte) {
	var info NodeInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return
	}

	// 更新已知节点列表
	n.nodesMutex.Lock()
	defer n.nodesMutex.Unlock()

	// 检查是否已存在
	existing, ok := n.knownNodes[datasync.NodeID(info.NodeID)]
	if ok {
		// 更新现有节点信息
		existing.IsLeader = info.IsLeader
		existing.Timestamp = info.Timestamp
	} else {
		// 添加新节点
		n.knownNodes[datasync.NodeID(info.NodeID)] = &info
	}
}

// DiscoverNodes 发现节点
func (n *NATS) DiscoverNodes(ctx context.Context, groupID datasync.GroupID) ([]datasync.NodeID, error) {
	n.nodesMutex.RLock()
	defer n.nodesMutex.RUnlock()

	// 过滤指定组的节点
	var nodes []datasync.NodeID
	for nodeID, info := range n.knownNodes {
		if info.GroupID == string(groupID) {
			// 检查节点是否过期（5分钟内没有更新视为过期）
			if time.Since(info.Timestamp) < 5*time.Minute {
				nodes = append(nodes, nodeID)
			}
		}
	}

	return nodes, nil
}

// GetKnownNodes 获取所有已知节点
func (n *NATS) GetKnownNodes() map[datasync.NodeID]*NodeInfo {
	n.nodesMutex.RLock()
	defer n.nodesMutex.RUnlock()

	// 复制一份避免外部修改
	result := make(map[datasync.NodeID]*NodeInfo, len(n.knownNodes))
	for k, v := range n.knownNodes {
		infoCopy := *v
		result[k] = &infoCopy
	}

	return result
}

// RemoveStaleNodes 清理过期节点
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

// SetErrorHandler 设置错误处理器
func (n *NATS) SetErrorHandler(handler ErrorHandler) {
	n.errorHandler = handler
}
