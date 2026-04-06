package transport

import (
	"context"
	"fmt"
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

// NATS NATS传输层实现
type NATS struct {
	config *NATSConfig
	conn   *nats.Conn
	js     nats.JetStreamContext
	subs   map[string]*nats.Subscription
	ctx    context.Context
	cancel context.CancelFunc
}

// NewNATS 创建NATS传输层
func NewNATS(config *NATSConfig) (*NATS, error) {
	// 初始化上下文
	ctx, cancel := context.WithCancel(context.Background())

	// 构建NATS连接选项
	opts := []nats.Option{
		nats.ReconnectWait(config.ReconnectWait),
		nats.MaxReconnects(config.MaxReconnects),
		nats.Timeout(config.ConnectionTimeout),
	}

	// 添加认证选项
	if config.Username != "" && config.Password != "" {
		opts = append(opts, nats.UserInfo(config.Username, config.Password))
	} else if config.Token != "" {
		opts = append(opts, nats.Token(config.Token))
	}

	// 添加TLS选项
	if config.TLSCert != "" && config.TLSKey != "" {
		tlsOpts := nats.ClientCert(config.TLSCert, config.TLSKey)
		opts = append(opts, tlsOpts)
	}

	if config.TLSCA != "" {
		tlsOpts := nats.RootCAs(config.TLSCA)
		opts = append(opts, tlsOpts)
	}

	// 连接到NATS
	conn, err := nats.Connect(config.Servers[0], opts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// 初始化NATS
	n := &NATS{
		config: config,
		conn:   conn,
		subs:   make(map[string]*nats.Subscription),
		ctx:    ctx,
		cancel: cancel,
	}

	// 启用JetStream
	if config.EnableJetStream {
		js, err := conn.JetStream()
		if err != nil {
			conn.Close()
			cancel()
			return nil, fmt.Errorf("failed to create JetStream context: %w", err)
		}
		n.js = js

		// 创建流
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     config.StreamName,
			Subjects: []string{"raftsync.*"},
		})
		if err != nil {
			// 忽略流已存在的错误
			conn.Close()
			cancel()
			return nil, fmt.Errorf("failed to create stream: %w", err)
		}
	}

	return n, nil
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
	// 取消上下文
	n.cancel()

	// 取消所有订阅
	for _, sub := range n.subs {
		sub.Unsubscribe()
	}

	// 关闭连接
	n.conn.Close()

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
		sub.Unsubscribe()
	}

	// 创建新的订阅
	sub, err := n.conn.Subscribe(subject, func(msg *nats.Msg) {
		callback(msg.Data)
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	n.subs[subject] = sub
	return nil
}

// DiscoverNodes 发现节点
func (n *NATS) DiscoverNodes(ctx context.Context, groupID datasync.GroupID) ([]datasync.NodeID, error) {
	// 这里可以实现节点发现逻辑
	// 例如，通过NATS的服务发现或其他机制
	// 暂时返回空列表
	return []datasync.NodeID{}, nil
}
