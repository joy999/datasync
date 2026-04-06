# Contributing to DataSync

感谢您对 DataSync 项目的关注！我们欢迎各种形式的贡献，包括但不限于：

- 提交 bug 报告
- 提交功能建议
- 提交代码修复
- 改进文档

## 开发环境准备

### 前置要求

- Go 1.21 或更高版本
- Make（可选，用于使用 Makefile）
- Protocol Buffers 编译器 (protoc)
- golangci-lint（用于代码检查）

### 安装依赖

```bash
# 下载项目依赖
go mod download

# 安装 protobuf 生成工具
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

## 构建和测试

### 使用 Make（推荐）

```bash
# 构建项目
make build

# 运行测试
make test

# 运行测试并生成覆盖率报告
make test-coverage

# 运行代码检查
make lint

# 自动修复代码检查问题
make lint-fix

# 格式化代码
make fmt

# 运行所有检查
make check
```

### 使用 Go 命令

```bash
# 构建
go build -v ./...

# 测试
go test -v -race ./...

# 代码检查
go vet ./...
```

## 提交代码

### 工作流程

1. Fork 本仓库
2. 创建您的特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交您的更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建一个 Pull Request

### 代码规范

- 所有代码必须通过 `golangci-lint` 检查
- 所有测试必须通过（包括竞态检测 `-race`）
- 保持代码覆盖率不降低
- 遵循 [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)

### 提交信息规范

提交信息应清晰描述更改内容：

```
type: subject

body（可选）
```

type 类型：
- `feat`: 新功能
- `fix`: 修复 bug
- `docs`: 文档更新
- `style`: 代码格式（不影响代码运行的变动）
- `refactor`: 重构
- `test`: 测试相关
- `chore`: 构建过程或辅助工具的变动

示例：
```
feat: add support for custom driver codecs

Implement driver-specific encoding/decoding to allow 
different serialization formats per driver.
```

## 测试指南

### 编写测试

- 测试文件应以 `_test.go` 结尾
- 使用表格驱动测试
- 覆盖率应尽可能高

```go
func TestMyFunction(t *testing.T) {
    tests := []struct {
        name     string
        input    string
        expected string
        wantErr  bool
    }{
        {
            name:     "valid input",
            input:    "test",
            expected: "TEST",
            wantErr:  false,
        },
        {
            name:     "empty input",
            input:    "",
            expected: "",
            wantErr:  true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result, err := MyFunction(tt.input)
            if (err != nil) != tt.wantErr {
                t.Errorf("MyFunction() error = %v, wantErr %v", err, tt.wantErr)
                return
            }
            if result != tt.expected {
                t.Errorf("MyFunction() = %v, want %v", result, tt.expected)
            }
        })
    }
}
```

### 基准测试

对于性能关键的代码，请添加基准测试：

```go
func BenchmarkMyFunction(b *testing.B) {
    for i := 0; i < b.N; i++ {
        MyFunction("test input")
    }
}
```

## Protobuf 规范

修改 `.proto` 文件后，需要重新生成 Go 代码：

```bash
make proto
```

确保生成的 `proto/datasync.pb.go` 文件已提交到版本控制。

## CI/CD

项目使用 GitHub Actions 进行持续集成。每次提交和 Pull Request 都会触发：

1. **Test**: 在多个 Go 版本 (1.21, 1.22, 1.23) 上运行测试
2. **Lint**: 运行 golangci-lint 代码检查
3. **Build**: 验证项目可以成功构建
4. **Proto Check**: 验证 protobuf 文件已更新

请确保您的更改在本地通过所有检查后再提交：

```bash
make check
```

## 报告问题

如果您发现了 bug 或有功能建议，请通过 GitHub Issues 提交。提交时请包含：

- 问题的清晰描述
- 复现步骤（如果是 bug）
- 期望的行为
- 实际的行为
- 环境信息（Go 版本、操作系统等）

## 许可证

通过提交代码，您同意您的贡献将根据项目的 [MIT 许可证](LICENSE) 进行许可。
