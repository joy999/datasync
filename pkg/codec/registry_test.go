package codec

import (
	"context"
	"testing"
	"time"

	datasync "github.com/joy999/datasync/pkg"
)

// MockDriver 用于测试的模拟驱动
type MockDriver struct {
	id      datasync.DriverID
	marshal func(*datasync.DataRecord) ([]byte, error)
}

func (m *MockDriver) Initialize(ctx context.Context, config map[string]interface{}) error {
	return nil
}
func (m *MockDriver) Close() error { return nil }
func (m *MockDriver) GetRecords(ctx context.Context, dataType string, cursor string, limit int) ([]*datasync.DataRecord, string, error) {
	return nil, "", nil
}
func (m *MockDriver) GetRecord(ctx context.Context, dataType string, id datasync.DataID) (*datasync.DataRecord, error) {
	return nil, nil
}
func (m *MockDriver) ApplyRecord(ctx context.Context, record *datasync.DataRecord) error {
	return nil
}
func (m *MockDriver) GetChanges(ctx context.Context, dataType string, since time.Time, limit int) ([]*datasync.DataRecord, error) {
	return nil, nil
}
func (m *MockDriver) GetDataTypes(ctx context.Context) ([]string, error) { return nil, nil }
func (m *MockDriver) GetLatestVersion(ctx context.Context, dataType string, id datasync.DataID) (int64, error) {
	return 0, nil
}
func (m *MockDriver) SetChangeCallback(callback func(*datasync.DataRecord)) {}
func (m *MockDriver) GetDriverID() datasync.DriverID                        { return m.id }
func (m *MockDriver) Marshal(record *datasync.DataRecord) ([]byte, error) {
	if m.marshal != nil {
		return m.marshal(record)
	}
	return []byte(`{"id":"` + string(record.ID) + `"}`), nil
}
func (m *MockDriver) Unmarshal(data []byte) (*datasync.DataRecord, error) {
	return &datasync.DataRecord{ID: "test"}, nil
}

func TestRegistry_Register(t *testing.T) {
	r := NewRegistry()

	driver1 := &MockDriver{id: 1}
	driver2 := &MockDriver{id: 2}

	// 注册第一个驱动
	if err := r.Register(driver1); err != nil {
		t.Fatalf("Register() first driver error = %v", err)
	}

	// 注册第二个驱动
	if err := r.Register(driver2); err != nil {
		t.Fatalf("Register() second driver error = %v", err)
	}

	// 重复注册应该失败
	if err := r.Register(&MockDriver{id: 1}); err == nil {
		t.Error("Register() duplicate driver should error")
	}

	// ID 为 0 应该失败
	if err := r.Register(&MockDriver{id: 0}); err == nil {
		t.Error("Register() driver ID 0 should error")
	}
}

func TestRegistry_Get(t *testing.T) {
	r := NewRegistry()
	driver := &MockDriver{id: 42}
	r.Register(driver)

	// 获取存在的驱动
	d, ok := r.Get(42)
	if !ok {
		t.Error("Get() should find registered driver")
	}
	if d.GetDriverID() != 42 {
		t.Errorf("Get() driver ID = %d, want 42", d.GetDriverID())
	}

	// 获取不存在的驱动
	_, ok = r.Get(99)
	if ok {
		t.Error("Get() should not find unregistered driver")
	}
}

func TestRegistry_EncodeDecode(t *testing.T) {
	r := NewRegistry()

	testRecord := &datasync.DataRecord{
		ID:      "test-1",
		Type:    "users",
		Version: 1,
		Payload: []byte(`{"name":"Alice"}`),
	}

	driver := &MockDriver{
		id: 100,
		marshal: func(r *datasync.DataRecord) ([]byte, error) {
			return []byte(`{"id":"` + string(r.ID) + `"}`), nil
		},
	}
	r.Register(driver)

	// 编码
	encoded, err := r.Encode(testRecord, 100)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	// 验证编码后的数据包含 DriverID（变长）
	// ID=100 应该是两字节编码: 0x80|0x00, 0x64
	if len(encoded) < 2 {
		t.Error("Encode() output too short")
	}

	// 解码
	decoded, err := r.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if decoded.ID != "test" {
		t.Errorf("Decode() ID = %v, want test", decoded.ID)
	}
}

func TestRegistry_EncodeUnknownDriver(t *testing.T) {
	r := NewRegistry()
	record := &datasync.DataRecord{ID: "test"}

	_, err := r.Encode(record, 999)
	if err == nil {
		t.Error("Encode() should error for unknown driver")
	}
}

func TestRegistry_DecodeUnknownDriver(t *testing.T) {
	r := NewRegistry()
	v := NewVarint()

	// 编码一个未注册的 DriverID
	idBytes, _ := v.Encode(999)
	data := append(idBytes, []byte(`{}`)...)

	_, err := r.Decode(data)
	if err == nil {
		t.Error("Decode() should error for unknown driver ID")
	}
}

func TestRegistry_DecodeEmpty(t *testing.T) {
	r := NewRegistry()

	_, err := r.Decode([]byte{})
	if err == nil {
		t.Error("Decode() should error for empty data")
	}
}

func TestRegistry_DecodeDriverID(t *testing.T) {
	r := NewRegistry()
	v := NewVarint()

	// 编码 ID = 1000（两字节，因为 128 <= 1000 <= 16383）
	idBytes, _ := v.Encode(1000)
	payload := []byte(`{"data":"test"}`)
	data := append(idBytes, payload...)

	driverID, n, err := r.DecodeDriverID(data)
	if err != nil {
		t.Fatalf("DecodeDriverID() error = %v", err)
	}
	if driverID != 1000 {
		t.Errorf("DecodeDriverID() ID = %d, want 1000", driverID)
	}
	if n != 2 {
		t.Errorf("DecodeDriverID() bytes = %d, want 2", n)
	}
}

func TestRegistry_GetDriverIDLen(t *testing.T) {
	r := NewRegistry()

	tests := []struct {
		id   datasync.DriverID
		want int
	}{
		{1, 1},
		{127, 1},
		{128, 2},
		{16383, 2},
		{16384, 3},
		{2097151, 3},
		{2097152, 4},
	}

	for _, tt := range tests {
		got := r.GetDriverIDLen(tt.id)
		if got != tt.want {
			t.Errorf("GetDriverIDLen(%d) = %d, want %d", tt.id, got, tt.want)
		}
	}
}

func TestGlobalRegistry(t *testing.T) {
	// 清理全局注册表（重新创建）
	globalRegistry = NewRegistry()

	driver := &MockDriver{id: 50}

	// 测试全局注册函数
	if err := Register(driver); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	// 测试全局解码（使用 Encode 需要驱动支持，这里测试 DecodeDriverID）
	v := NewVarint()
	idBytes, _ := v.Encode(50)
	data := append(idBytes, []byte(`{}`)...)

	driverID, _, err := DecodeDriverID(data)
	if err != nil {
		t.Fatalf("DecodeDriverID() error = %v", err)
	}
	if driverID != 50 {
		t.Errorf("DecodeDriverID() = %d, want 50", driverID)
	}
}
