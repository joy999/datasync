package sync

import (
	"context"
	"testing"

	datasync "github.com/joy999/datasync/pkg"
	pb "github.com/joy999/datasync/proto"
)

type testTransport struct {
	callback func([]byte)
	messages [][]byte
}

func (t *testTransport) Start(ctx context.Context) error { return nil }
func (t *testTransport) Stop() error                     { return nil }
func (t *testTransport) SendMessage(ctx context.Context, targetGroup datasync.GroupID, message []byte) error {
	t.messages = append(t.messages, message)
	return nil
}
func (t *testTransport) Subscribe(ctx context.Context, groupID datasync.GroupID, callback func([]byte)) error {
	t.callback = callback
	return nil
}
func (t *testTransport) DiscoverNodes(ctx context.Context, groupID datasync.GroupID) ([]datasync.NodeID, error) {
	return nil, nil
}

func TestManagerRejectsInvalidAuth(t *testing.T) {
	transport := &testTransport{}
	manager, err := NewManager(context.Background(), "node-1", datasync.GroupID("group-a"), transport)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	manager.SetAuthValidator(func(token string) bool {
		return token == "valid-token"
	})
	manager.SetAuthToken("invalid-token")

	data, err := manager.codec.EncodeHeartbeat(&pb.Heartbeat{
		NodeId:  "node-2",
		GroupId: "group-b",
	})
	if err != nil {
		t.Fatalf("EncodeHeartbeat() error = %v", err)
	}

	transport.callback(data)

	if len(manager.heartbeatTargets()) != 1 {
		t.Fatalf("无效认证不应新增已知组")
	}
}

func TestManagerTracksResponseByRequestID(t *testing.T) {
	transport := &testTransport{}
	manager, err := NewManager(context.Background(), "node-1", datasync.GroupID("group-a"), transport)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	targetGroup := datasync.GroupID("group-b")
	if err := manager.TriggerSync(context.Background(), targetGroup, []string{"users"}); err != nil {
		t.Fatalf("TriggerSync() error = %v", err)
	}

	var requestID string
	for id := range manager.requestMap {
		requestID = id
	}
	if requestID == "" {
		t.Fatalf("预期请求 ID 已被跟踪")
	}

	manager.processSyncResponse(&pb.SyncResponse{
		RequestId: requestID,
		Success:   true,
	})

	status, err := manager.GetSyncStatus(context.Background(), datasync.GroupID("group-a"), targetGroup)
	if err != nil {
		t.Fatalf("GetSyncStatus() error = %v", err)
	}
	if status.LastSyncError != "" {
		t.Fatalf("预期同步错误为空，实际为 %q", status.LastSyncError)
	}
	if len(manager.requestMap) != 0 {
		t.Fatalf("预期请求映射已被清空")
	}
}
