package codec

import (
	"encoding/binary"
	"fmt"

	datasync "github.com/joy999/datasync/pkg"
)

// Varint 变长整数编解码
// 编码规则（大端序）：
// - 0xxxxxxx                    : 单字节，0-127
// - 10xxxxxx xxxxxxxx           : 两字节，0-16383 (14位)
// - 110xxxxx xxxxxxxx xxxxxxxx  : 三字节，0-2097151 (21位)
// - 1110xxxx ...                : 四字节，0-268435455 (28位)
// - 11110xxx ...                : 五字节，0-34359738367 (35位)
// - 以此类推，最多 5 字节（35位足够使用）

type Varint struct{}

// NewVarint 创建变长编码器
func NewVarint() *Varint {
	return &Varint{}
}

// Encode 将 DriverID 编码为变长字节
func (v *Varint) Encode(id datasync.DriverID) ([]byte, error) {
	if id <= 0x7F {
		// 单字节: 0xxxxxxx
		return []byte{byte(id)}, nil
	} else if id <= 0x3FFF {
		// 两字节: 10xxxxxx xxxxxxxx
		return []byte{
			0x80 | byte((id>>8)&0x3F),
			byte(id & 0xFF),
		}, nil
	} else if id <= 0x1FFFFF {
		// 三字节: 110xxxxx xxxxxxxx xxxxxxxx
		return []byte{
			0xC0 | byte((id>>16)&0x1F),
			byte((id >> 8) & 0xFF),
			byte(id & 0xFF),
		}, nil
	} else if id <= 0xFFFFFFF {
		// 四字节: 1110xxxx xxxxxxxx xxxxxxxx xxxxxxxx
		return []byte{
			0xE0 | byte((id>>24)&0x0F),
			byte((id >> 16) & 0xFF),
			byte((id >> 8) & 0xFF),
			byte(id & 0xFF),
		}, nil
	} else if id <= 0x7FFFFFFF {
		// 五字节: 11110xxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
		// 注意：由于 DriverID 是 uint32，最大值为 0xFFFFFFFF
		// 这里使用 0x7FFFFFFF 作为 uint32 能表示的最大值（31位）
		return []byte{
			0xF0 | byte((id>>28)&0x07),
			byte((id >> 24) & 0xFF),
			byte((id >> 16) & 0xFF),
			byte((id >> 8) & 0xFF),
			byte(id & 0xFF),
		}, nil
	}
	return nil, fmt.Errorf("driver ID %d exceeds maximum value (0x7FFFFFFF)", id)
}

// Decode 从变长字节解码 DriverID
// 返回解码后的 ID 和占用的字节数
func (v *Varint) Decode(data []byte) (datasync.DriverID, int, error) {
	if len(data) == 0 {
		return 0, 0, fmt.Errorf("empty data")
	}

	first := data[0]

	// 检查最高位
	if first&0x80 == 0 {
		// 单字节: 0xxxxxxx
		return datasync.DriverID(first), 1, nil
	} else if first&0xC0 == 0x80 {
		// 两字节: 10xxxxxx xxxxxxxx
		if len(data) < 2 {
			return 0, 0, fmt.Errorf("insufficient data for 2-byte ID")
		}
		id := datasync.DriverID((first&0x3F))<<8 | datasync.DriverID(data[1])
		return id, 2, nil
	} else if first&0xE0 == 0xC0 {
		// 三字节: 110xxxxx xxxxxxxx xxxxxxxx
		if len(data) < 3 {
			return 0, 0, fmt.Errorf("insufficient data for 3-byte ID")
		}
		id := datasync.DriverID((first & 0x1F)) << 16
		id |= datasync.DriverID(data[1]) << 8
		id |= datasync.DriverID(data[2])
		return id, 3, nil
	} else if first&0xF0 == 0xE0 {
		// 四字节: 1110xxxx xxxxxxxx xxxxxxxx xxxxxxxx
		if len(data) < 4 {
			return 0, 0, fmt.Errorf("insufficient data for 4-byte ID")
		}
		id := datasync.DriverID((first & 0x0F)) << 24
		id |= datasync.DriverID(data[1]) << 16
		id |= datasync.DriverID(data[2]) << 8
		id |= datasync.DriverID(data[3])
		return id, 4, nil
	} else if first&0xF8 == 0xF0 {
		// 五字节: 11110xxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
		if len(data) < 5 {
			return 0, 0, fmt.Errorf("insufficient data for 5-byte ID")
		}
		id := datasync.DriverID((first & 0x07)) << 28
		id |= datasync.DriverID(data[1]) << 24
		id |= datasync.DriverID(data[2]) << 16
		id |= datasync.DriverID(data[3]) << 8
		id |= datasync.DriverID(data[4])
		return id, 5, nil
	}

	return 0, 0, fmt.Errorf("invalid driver ID encoding: first byte 0x%02X", first)
}

// MaxLen 返回变长编码的最大长度（字节）
func (v *Varint) MaxLen() int {
	return 5
}

// EncodeToUint64 辅助函数：将 DriverID 编码到 uint64 的低 40 位（用于存储）
func EncodeToUint64(id datasync.DriverID) uint64 {
	return uint64(id)
}

// DecodeFromUint64 辅助函数：从 uint64 解码 DriverID
func DecodeFromUint64(val uint64) datasync.DriverID {
	if val > 0xFFFFFFFF {
		val = 0xFFFFFFFF
	}
	// nolint:gosec // val is checked to be within uint32 range
	return datasync.DriverID(val)
}

// BinaryEncode 使用固定4字节大端序编码（适用于网络传输场景）
func BinaryEncode(id datasync.DriverID) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(id))
	return buf
}

// BinaryDecode 解码固定4字节大端序
func BinaryDecode(data []byte) (datasync.DriverID, error) {
	if len(data) < 4 {
		return 0, fmt.Errorf("insufficient data for binary decode")
	}
	return datasync.DriverID(binary.BigEndian.Uint32(data[:4])), nil
}
