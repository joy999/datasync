package codec

import (
	"context"
	"fmt"
	"testing"
	"time"

	datasync "github.com/joy999/datasync/pkg"
)

// BenchmarkVarint_EncodeSmall 小数值编码基准测试
func BenchmarkVarint_EncodeSmall(b *testing.B) {
	v := NewVarint()
	id := datasync.DriverID(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = v.Encode(id)
	}
}

// BenchmarkVarint_EncodeMedium 中等数值编码基准测试
func BenchmarkVarint_EncodeMedium(b *testing.B) {
	v := NewVarint()
	id := datasync.DriverID(10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = v.Encode(id)
	}
}

// BenchmarkVarint_EncodeLarge 大数值编码基准测试
func BenchmarkVarint_EncodeLarge(b *testing.B) {
	v := NewVarint()
	id := datasync.DriverID(10000000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = v.Encode(id)
	}
}

// BenchmarkVarint_DecodeSmall 小数值解码基准测试
func BenchmarkVarint_DecodeSmall(b *testing.B) {
	v := NewVarint()
	data, _ := v.Encode(datasync.DriverID(100))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = v.Decode(data)
	}
}

// BenchmarkVarint_DecodeMedium 中等数值解码基准测试
func BenchmarkVarint_DecodeMedium(b *testing.B) {
	v := NewVarint()
	data, _ := v.Encode(datasync.DriverID(10000))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = v.Decode(data)
	}
}

// BenchmarkVarint_DecodeLarge 大数值解码基准测试
func BenchmarkVarint_DecodeLarge(b *testing.B) {
	v := NewVarint()
	data, _ := v.Encode(datasync.DriverID(10000000))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = v.Decode(data)
	}
}

// BenchmarkRegistry_Register 注册驱动基准测试
func BenchmarkRegistry_Register(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry := NewRegistry()
		driver := &benchMockDriver{id: datasync.DriverID(i%255 + 1)}
		_ = registry.Register(driver)
	}
}

// BenchmarkRegistry_Encode 编码基准测试
func BenchmarkRegistry_Encode(b *testing.B) {
	registry := NewRegistry()
	driver := &benchMockDriver{id: 1}
	_ = registry.Register(driver)

	record := &datasync.DataRecord{
		ID:      "test-record",
		Type:    "users",
		Version: 1,
		Payload: []byte(`{"name":"Alice","email":"alice@example.com"}`),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = registry.Encode(record, 1)
	}
}

// BenchmarkRegistry_Decode 解码基准测试
func BenchmarkRegistry_Decode(b *testing.B) {
	registry := NewRegistry()
	driver := &benchMockDriver{id: 1}
	_ = registry.Register(driver)

	record := &datasync.DataRecord{
		ID:      "test-record",
		Type:    "users",
		Version: 1,
		Payload: []byte(`{"name":"Alice","email":"alice@example.com"}`),
	}
	data, _ := registry.Encode(record, 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = registry.Decode(data)
	}
}

// BenchmarkRegistry_EncodeDecode 编解码完整流程基准测试
func BenchmarkRegistry_EncodeDecode(b *testing.B) {
	registry := NewRegistry()
	driver := &benchMockDriver{id: 1}
	_ = registry.Register(driver)

	record := &datasync.DataRecord{
		ID:      "test-record",
		Type:    "users",
		Version: 1,
		Payload: []byte(`{"name":"Alice","email":"alice@example.com"}`),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, _ := registry.Encode(record, 1)
		_, _ = registry.Decode(data)
	}
}

// BenchmarkComparison_BinaryVsVarint 对比二进制和变长编码
func BenchmarkComparison_BinaryVsVarint(b *testing.B) {
	ids := []datasync.DriverID{1, 100, 10000, 1000000}

	b.Run("Binary", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, id := range ids {
				_ = BinaryEncode(id)
			}
		}
	})

	v := NewVarint()
	b.Run("Varint", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, id := range ids {
				_, _ = v.Encode(id)
			}
		}
	})
}

// 不同 payload 大小的编码基准测试
func BenchmarkRegistry_DifferentPayloadSizes(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Payload%d", size), func(b *testing.B) {
			registry := NewRegistry()
			driver := &benchMockDriver{id: 1}
			_ = registry.Register(driver)

			payload := make([]byte, size)
			record := &datasync.DataRecord{
				ID:      "test",
				Type:    "test",
				Version: 1,
				Payload: payload,
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, _ := registry.Encode(record, 1)
				_, _ = registry.Decode(data)
			}
		})
	}
}

// 并行基准测试
func BenchmarkRegistry_EncodeDecode_Parallel(b *testing.B) {
	registry := NewRegistry()
	driver := &benchMockDriver{id: 1}
	_ = registry.Register(driver)

	record := &datasync.DataRecord{
		ID:      "test-record",
		Type:    "users",
		Version: 1,
		Payload: []byte(`{"name":"Alice"}`),
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			data, _ := registry.Encode(record, 1)
			_, _ = registry.Decode(data)
		}
	})
}

// benchMockDriver 基准测试用的模拟驱动
type benchMockDriver struct {
	id datasync.DriverID
}

func (m *benchMockDriver) GetDriverID() datasync.DriverID { return m.id }
func (m *benchMockDriver) Marshal(r *datasync.DataRecord) ([]byte, error) {
	return r.Payload, nil
}
func (m *benchMockDriver) Unmarshal(d []byte) (*datasync.DataRecord, error) {
	return &datasync.DataRecord{Payload: d}, nil
}
func (m *benchMockDriver) Initialize(_ context.Context, _ map[string]interface{}) error { return nil }
func (m *benchMockDriver) Close() error                                                 { return nil }
func (m *benchMockDriver) GetRecords(_ context.Context, _ string, _ string, _ int) ([]*datasync.DataRecord, string, error) {
	return nil, "", nil
}
func (m *benchMockDriver) GetRecord(_ context.Context, _ string, _ datasync.DataID) (*datasync.DataRecord, error) {
	return nil, nil
}
func (m *benchMockDriver) ApplyRecord(_ context.Context, _ *datasync.DataRecord) error { return nil }
func (m *benchMockDriver) GetChanges(_ context.Context, _ string, _ time.Time, _ int) ([]*datasync.DataRecord, error) {
	return nil, nil
}
func (m *benchMockDriver) GetDataTypes(_ context.Context) ([]string, error) { return nil, nil }
func (m *benchMockDriver) GetLatestVersion(_ context.Context, _ string, _ datasync.DataID) (int64, error) {
	return 0, nil
}
func (m *benchMockDriver) SetChangeCallback(_ func(*datasync.DataRecord)) {}
