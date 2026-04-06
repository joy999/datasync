package codec

import (
	"testing"

	datasync "github.com/joy999/datasync/pkg"
)

func TestVarint_EncodeDecode(t *testing.T) {
	v := NewVarint()

	tests := []struct {
		name    string
		id      datasync.DriverID
		wantLen int
		wantErr bool
	}{
		{"zero (reserved)", 0, 1, false},
		{"single byte min", 1, 1, false},
		{"single byte max", 127, 1, false},
		{"two bytes min", 128, 2, false},
		{"two bytes mid", 8192, 2, false},
		{"two bytes max", 16383, 2, false},
		{"three bytes min", 16384, 3, false},
		{"three bytes mid", 100000, 3, false},
		{"three bytes max", 2097151, 3, false},
		{"four bytes min", 2097152, 4, false},
		{"four bytes mid", 100000000, 4, false},
		{"four bytes max", 268435455, 4, false},
		{"five bytes min", 268435456, 5, false},
		{"five bytes mid", 1000000000, 5, false},
		{"five bytes max (uint32)", 0x7FFFFFFF, 5, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 测试编码
			encoded, err := v.Encode(tt.id)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Encode() error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(encoded) != tt.wantLen {
				t.Errorf("Encode() length = %d, want %d", len(encoded), tt.wantLen)
			}

			// 测试解码
			decoded, n, err := v.Decode(encoded)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if n != tt.wantLen {
				t.Errorf("Decode() bytes read = %d, want %d", n, tt.wantLen)
			}
			if decoded != tt.id {
				t.Errorf("Decode() = %d, want %d", decoded, tt.id)
			}
		})
	}
}

func TestVarint_DecodeInsufficientData(t *testing.T) {
	v := NewVarint()

	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"2-byte marker with 1 byte", []byte{0x80}},
		{"3-byte marker with 2 bytes", []byte{0xC0, 0x00}},
		{"4-byte marker with 3 bytes", []byte{0xE0, 0x00, 0x00}},
		{"5-byte marker with 4 bytes", []byte{0xF0, 0x00, 0x00, 0x00}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := v.Decode(tt.data)
			if err == nil {
				t.Error("Decode() expected error for insufficient data")
			}
		})
	}
}

func TestVarint_DecodeInvalidEncoding(t *testing.T) {
	v := NewVarint()

	// 测试无效的前缀（11111xxx 是保留的）
	data := []byte{0xF8, 0x00, 0x00, 0x00, 0x00}
	_, _, err := v.Decode(data)
	if err == nil {
		t.Error("Decode() expected error for invalid encoding prefix")
	}
}

func TestVarint_MaxLen(t *testing.T) {
	v := NewVarint()
	if v.MaxLen() != 5 {
		t.Errorf("MaxLen() = %d, want 5", v.MaxLen())
	}
}

func TestBinaryEncodeDecode(t *testing.T) {
	tests := []datasync.DriverID{0, 1, 127, 128, 255, 256, 65535, 65536, 0x7FFFFFFF}

	for _, id := range tests {
		encoded := BinaryEncode(id)
		if len(encoded) != 4 {
			t.Errorf("BinaryEncode(%d) length = %d, want 4", id, len(encoded))
		}

		decoded, err := BinaryDecode(encoded)
		if err != nil {
			t.Fatalf("BinaryDecode() error = %v", err)
		}
		if decoded != id {
			t.Errorf("BinaryDecode() = %d, want %d", decoded, id)
		}
	}
}

func TestBinaryDecodeInsufficientData(t *testing.T) {
	_, err := BinaryDecode([]byte{0x00, 0x00, 0x00}) // 只有3字节
	if err == nil {
		t.Error("BinaryDecode() expected error for insufficient data")
	}
}

func BenchmarkVarint_Encode(b *testing.B) {
	v := NewVarint()
	id := datasync.DriverID(100000) // 三字节编码

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = v.Encode(id)
	}
}

func BenchmarkVarint_Decode(b *testing.B) {
	v := NewVarint()
	id := datasync.DriverID(100000)
	encoded, _ := v.Encode(id)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = v.Decode(encoded)
	}
}
