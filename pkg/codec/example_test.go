package codec_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/joy999/datasync/pkg/codec"
	datasync "github.com/joy999/datasync/pkg"
)

// ExampleRegistry_Register 展示如何注册驱动
func ExampleRegistry_Register() {
	// 创建一个内存驱动的模拟
	driver := &MockDriver{id: 1}

	// 注册驱动到注册表
	registry := codec.NewRegistry()
	if err := registry.Register(driver); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Driver registered successfully")
	// Output: Driver registered successfully
}

// ExampleRegistry_encodeDecode 展示编码和解码数据
func ExampleRegistry_encodeDecode() {
	// 创建并注册驱动
	driver := &MockDriver{id: 1}
	registry := codec.NewRegistry()
	_ = registry.Register(driver)

	// 创建数据记录
	record := &datasync.DataRecord{
		ID:      "user-1",
		Type:    "users",
		Version: 1,
		Payload: []byte(`{"name":"Alice"}`),
	}

	// 编码数据
	encoded, err := registry.Encode(record, 1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Encoded successfully (%d bytes)\n", len(encoded))

	// 解码数据
	decoded, err := registry.Decode(encoded)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Decoded record ID: %s\n", decoded.ID)

	// Output:
	// Encoded successfully (7 bytes)
	// Decoded record ID: user-1
}

// ExampleVarint_Encode 展示变长编码
func ExampleVarint_Encode() {
	v := codec.NewVarint()

	// 编码小数值（1字节）
	data1, _ := v.Encode(100)
	fmt.Printf("ID=100 encodes to %d bytes\n", len(data1))

	// 编码中等数值（2字节）
	data2, _ := v.Encode(1000)
	fmt.Printf("ID=1000 encodes to %d bytes\n", len(data2))

	// 编码大数值（3字节）
	data3, _ := v.Encode(100000)
	fmt.Printf("ID=100000 encodes to %d bytes\n", len(data3))

	// Output:
	// ID=100 encodes to 1 bytes
	// ID=1000 encodes to 2 bytes
	// ID=100000 encodes to 3 bytes
}

// ExampleVarint_Decode 展示变长解码
func ExampleVarint_Decode() {
	v := codec.NewVarint()

	// 编码数据
	data, _ := v.Encode(12345)

	// 解码数据
	id, bytesRead, err := v.Decode(data)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Decoded ID: %d (read %d bytes)\n", id, bytesRead)
	// Output: Decoded ID: 12345 (read 2 bytes)
}

// MockDriver 用于示例的模拟驱动
type MockDriver struct {
	id datasync.DriverID
}

func (m *MockDriver) GetDriverID() datasync.DriverID { return m.id }
func (m *MockDriver) Marshal(r *datasync.DataRecord) ([]byte, error) {
	return []byte(r.ID), nil
}
func (m *MockDriver) Unmarshal(d []byte) (*datasync.DataRecord, error) {
	return &datasync.DataRecord{ID: datasync.DataID(d)}, nil
}
func (m *MockDriver) Initialize(_ context.Context, _ map[string]interface{}) error { return nil }
func (m *MockDriver) Close() error                                               { return nil }
func (m *MockDriver) GetRecords(_ context.Context, _ string, _ string, _ int) ([]*datasync.DataRecord, string, error) {
	return nil, "", nil
}
func (m *MockDriver) GetRecord(_ context.Context, _ string, _ datasync.DataID) (*datasync.DataRecord, error) {
	return nil, nil
}
func (m *MockDriver) ApplyRecord(_ context.Context, _ *datasync.DataRecord) error { return nil }
func (m *MockDriver) GetChanges(_ context.Context, _ string, _ time.Time, _ int) ([]*datasync.DataRecord, error) {
	return nil, nil
}
func (m *MockDriver) GetDataTypes(_ context.Context) ([]string, error) { return nil, nil }
func (m *MockDriver) GetLatestVersion(_ context.Context, _ string, _ datasync.DataID) (int64, error) {
	return 0, nil
}
func (m *MockDriver) SetChangeCallback(_ func(*datasync.DataRecord)) {}
