package raft

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	datasync "github.com/joy999/datasync/pkg"
	"github.com/joy999/datasync/pkg/codec"
	"github.com/joy999/datasync/pkg/protocol"
	pb "github.com/joy999/datasync/proto"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Config 表示 Raft 节点配置。
type Config struct {
	NodeID           datasync.NodeID
	GroupID          datasync.GroupID
	DataDir          string
	Transport        datasync.Transport
	HeartbeatTick    int
	ElectionTick     int
	SnapshotInterval time.Duration
	MaxSnapshotFiles int
}

// Node 封装了一个 etcd/raft 节点。
type Node struct {
	config      *Config
	raftNode    raft.Node
	raftStorage *raft.MemoryStorage
	peerIDs     []uint64
	dataDir     string
	transport   datasync.Transport
	isLeader    bool
	raftConfig  *raft.Config
	codec       *protocol.Codec
	ctx         context.Context
	cancel      context.CancelFunc

	applyCh      chan *datasync.DataRecord
	appliedIndex uint64
	mu           sync.RWMutex
}

// NewNode 创建一个新的 Raft 节点。
func NewNode(config *Config) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())

	dataDir := filepath.Join(config.DataDir, string(config.GroupID))
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	storage := raft.NewMemoryStorage()
	raftConfig := &raft.Config{
		ID:              uint64(config.NodeID[0]),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}
	if config.ElectionTick > 0 {
		raftConfig.ElectionTick = config.ElectionTick
	}
	if config.HeartbeatTick > 0 {
		raftConfig.HeartbeatTick = config.HeartbeatTick
	}

	peers := []raft.Peer{{ID: uint64(config.NodeID[0])}}
	node := raft.StartNode(raftConfig, peers)

	rn := &Node{
		config:       config,
		raftNode:     node,
		raftStorage:  storage,
		peerIDs:      []uint64{uint64(config.NodeID[0])},
		dataDir:      dataDir,
		transport:    config.Transport,
		isLeader:     false,
		raftConfig:   raftConfig,
		codec:        protocol.NewCodec(&protocol.Config{NodeID: string(config.NodeID), GroupID: string(config.GroupID)}),
		ctx:          ctx,
		cancel:       cancel,
		applyCh:      make(chan *datasync.DataRecord, 1000),
		appliedIndex: 0,
	}

	if config.Transport != nil {
		if err := rn.startSubscription(); err != nil {
			cancel()
			node.Stop()
			return nil, fmt.Errorf("failed to subscribe raft transport: %w", err)
		}
	}

	go rn.processMessages()
	return rn, nil
}

// Start 启动 Raft 节点，当前底层节点已在构造时启动。
func (n *Node) Start(ctx context.Context) error {
	return nil
}

// Stop 停止 Raft 节点。
func (n *Node) Stop() error {
	n.cancel()
	n.raftNode.Stop()
	close(n.applyCh)
	return nil
}

// IsLeader 返回当前节点是否为 Leader。
func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.isLeader
}

// GetAppliedIndex 返回最后已应用的 Raft 索引。
func (n *Node) GetAppliedIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.appliedIndex
}

// GetApplyChannel 返回状态机应用通道。
func (n *Node) GetApplyChannel() <-chan *datasync.DataRecord {
	return n.applyCh
}

// Apply 使用给定驱动 ID 将记录提议到 Raft。
func (n *Node) Apply(record *datasync.DataRecord, driverID datasync.DriverID) error {
	data, err := codec.Encode(record, driverID)
	if err != nil {
		return fmt.Errorf("failed to encode record: %w", err)
	}

	return n.raftNode.Propose(n.ctx, data)
}

// AddPeer 向本地 Raft 配置中添加对等节点。
func (n *Node) AddPeer(ctx context.Context, nodeID datasync.NodeID) error {
	peerID := uint64(nodeID[0])
	for _, id := range n.peerIDs {
		if id == peerID {
			return nil
		}
	}

	if n.isLeader {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := n.raftNode.ProposeConfChange(ctx, raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: peerID}); err != nil {
			return fmt.Errorf("failed to add peer: %w", err)
		}
	}

	n.peerIDs = append(n.peerIDs, peerID)
	return nil
}

// RemovePeer 从本地 Raft 配置中移除对等节点。
func (n *Node) RemovePeer(ctx context.Context, nodeID datasync.NodeID) error {
	peerID := uint64(nodeID[0])

	found := false
	for _, id := range n.peerIDs {
		if id == peerID {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("peer not found: %s", nodeID)
	}

	if n.isLeader {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := n.raftNode.ProposeConfChange(ctx, raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode, NodeID: peerID}); err != nil {
			return fmt.Errorf("failed to remove peer: %w", err)
		}
	}

	newPeers := make([]uint64, 0, len(n.peerIDs)-1)
	for _, id := range n.peerIDs {
		if id != peerID {
			newPeers = append(newPeers, id)
		}
	}
	n.peerIDs = newPeers

	return nil
}

func (n *Node) processMessages() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case ready := <-n.raftNode.Ready():
			if ready.SoftState != nil {
				n.mu.Lock()
				n.isLeader = ready.SoftState.Lead == n.raftConfig.ID
				n.mu.Unlock()
			}

			if len(ready.Entries) > 0 {
				_ = n.raftStorage.Append(ready.Entries)
			}
			if ready.Snapshot.Metadata.Term != 0 || ready.Snapshot.Metadata.Index != 0 {
				_ = n.raftStorage.ApplySnapshot(ready.Snapshot)
			}
			if len(ready.Messages) > 0 {
				n.sendMessages(ready.Messages)
			}

			for _, entry := range ready.CommittedEntries {
				if entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
					n.applyEntry(entry)
				}
				n.mu.Lock()
				n.appliedIndex = entry.Index
				n.mu.Unlock()
			}

			n.raftNode.Advance()
		}
	}
}

func (n *Node) sendMessages(msgs []raftpb.Message) {
	if n.transport == nil {
		return
	}

	for _, msg := range msgs {
		payload, err := msg.Marshal()
		if err != nil {
			continue
		}

		raftMsg := &pb.RaftMessage{
			Type:  convertRaftMessageType(msg.Type),
			Data:  payload,
			Term:  msg.Term,
			Index: msg.Index,
		}
		data, err := n.codec.EncodeRaftMessage(raftMsg)
		if err != nil {
			continue
		}
		if err := n.transport.SendMessage(n.ctx, n.config.GroupID, data); err != nil {
			continue
		}
	}
}

// HandleRaftMessage 将一条 Raft 消息投递给本地节点。
func (n *Node) HandleRaftMessage(msg raftpb.Message) error {
	if err := n.raftNode.Step(n.ctx, msg); err != nil {
		return fmt.Errorf("failed to step raft message: %w", err)
	}
	return nil
}

func (n *Node) applyEntry(entry raftpb.Entry) {
	record, err := codec.Decode(entry.Data)
	if err != nil {
		return
	}

	select {
	case n.applyCh <- record:
	default:
	}
}

// CreateSnapshot 创建内存快照。
func (n *Node) CreateSnapshot() (uint64, raftpb.Snapshot, error) {
	n.mu.RLock()
	appliedIndex := n.appliedIndex
	n.mu.RUnlock()

	snapshot, err := n.raftStorage.CreateSnapshot(appliedIndex, nil, nil)
	if err != nil {
		return 0, raftpb.Snapshot{}, fmt.Errorf("failed to create snapshot: %w", err)
	}

	return appliedIndex, snapshot, nil
}

func (n *Node) startSubscription() error {
	return n.transport.Subscribe(n.ctx, n.config.GroupID, n.handleTransportMessage)
}

func (n *Node) handleTransportMessage(data []byte) {
	env, raftMsg, err := n.codec.DecodeRaftMessage(data)
	if err != nil {
		return
	}
	if env.SourceNode == string(n.config.NodeID) {
		return
	}

	var msg raftpb.Message
	if err := msg.Unmarshal(raftMsg.Data); err != nil {
		return
	}

	_ = n.HandleRaftMessage(msg)
}

func convertRaftMessageType(msgType raftpb.MessageType) pb.RaftMessage_MessageType {
	switch msgType {
	case raftpb.MsgSnap:
		return pb.RaftMessage_SNAPSHOT
	case raftpb.MsgProp, raftpb.MsgApp, raftpb.MsgAppResp, raftpb.MsgHeartbeat, raftpb.MsgHeartbeatResp:
		return pb.RaftMessage_ENTRY
	default:
		return pb.RaftMessage_CONFIG
	}
}
