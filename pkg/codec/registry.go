package codec

import (
	"fmt"
	"sync"

	datasync "github.com/joy999/datasync/pkg"
)

// Registry 驱动注册表，用于管理驱动编解码器
type Registry struct {
	drivers map[datasync.DriverID]datasync.StorageDriver
	varint  *Varint
	mutex   sync.RWMutex
}

// globalRegistry 全局注册表实例
var globalRegistry = NewRegistry()

// NewRegistry 创建新的驱动注册表
func NewRegistry() *Registry {
	return &Registry{
		drivers: make(map[datasync.DriverID]datasync.StorageDriver),
		varint:  NewVarint(),
	}
}

// Register 注册驱动到注册表
func (r *Registry) Register(driver datasync.StorageDriver) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	driverID := driver.GetDriverID()
	if driverID == 0 {
		return fmt.Errorf("driver ID cannot be 0 (reserved)")
	}

	if _, exists := r.drivers[driverID]; exists {
		return fmt.Errorf("driver ID %d already registered", driverID)
	}

	r.drivers[driverID] = driver
	return nil
}

// Get 根据驱动ID获取驱动
func (r *Registry) Get(driverID datasync.DriverID) (datasync.StorageDriver, bool) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	driver, ok := r.drivers[driverID]
	return driver, ok
}

// Encode 使用指定驱动编码数据记录
// 返回格式: [变长 DriverID][N字节 Payload]
func (r *Registry) Encode(record *datasync.DataRecord, driverID datasync.DriverID) ([]byte, error) {
	driver, ok := r.Get(driverID)
	if !ok {
		return nil, fmt.Errorf("driver not found for ID: %d", driverID)
	}

	// 使用驱动 Marshal 数据
	payload, err := driver.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal record: %w", err)
	}

	// 编码 DriverID（变长）
	idBytes, err := r.varint.Encode(driverID)
	if err != nil {
		return nil, fmt.Errorf("failed to encode driver ID: %w", err)
	}

	// 组合: Varint(DriverID) + Payload
	data := make([]byte, len(idBytes)+len(payload))
	copy(data, idBytes)
	copy(data[len(idBytes):], payload)

	return data, nil
}

// Decode 解码数据
// 数据格式: [变长 DriverID][N字节 Payload]
func (r *Registry) Decode(data []byte) (*datasync.DataRecord, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	// 解码 DriverID（变长）
	driverID, idLen, err := r.varint.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode driver ID: %w", err)
	}

	payload := data[idLen:]

	driver, ok := r.Get(driverID)
	if !ok {
		return nil, fmt.Errorf("unknown driver ID: %d", driverID)
	}

	record, err := driver.Unmarshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal record with driver %d: %w", driverID, err)
	}

	return record, nil
}

// DecodeDriverID 只解码 DriverID，不解码完整数据
// 用于预览或路由场景
func (r *Registry) DecodeDriverID(data []byte) (datasync.DriverID, int, error) {
	if len(data) == 0 {
		return 0, 0, fmt.Errorf("empty data")
	}
	return r.varint.Decode(data)
}

// EncodeDriverID 只编码 DriverID
func (r *Registry) EncodeDriverID(driverID datasync.DriverID) ([]byte, error) {
	return r.varint.Encode(driverID)
}

// GetDriverIDLen 返回指定 DriverID 编码后的字节长度
func (r *Registry) GetDriverIDLen(driverID datasync.DriverID) int {
	bytes, _ := r.varint.Encode(driverID)
	return len(bytes)
}

// Global 获取全局注册表实例
func Global() *Registry {
	return globalRegistry
}

// Register 向全局注册表注册驱动
func Register(driver datasync.StorageDriver) error {
	return globalRegistry.Register(driver)
}

// Encode 使用全局注册表编码数据
func Encode(record *datasync.DataRecord, driverID datasync.DriverID) ([]byte, error) {
	return globalRegistry.Encode(record, driverID)
}

// Decode 使用全局注册表解码数据
func Decode(data []byte) (*datasync.DataRecord, error) {
	return globalRegistry.Decode(data)
}

// DecodeDriverID 使用全局注册表只解码 DriverID
func DecodeDriverID(data []byte) (datasync.DriverID, int, error) {
	return globalRegistry.DecodeDriverID(data)
}
