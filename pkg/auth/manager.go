package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	datasync "github.com/joy999/datasync/pkg"
)

// Config 认证配置
type Config struct {
	DefaultMethod  datasync.AuthMethod // 默认认证方法
	TokenSecret    string              // Token密钥
	TokenTTL       time.Duration       // Token过期时间
	CACertFile     string              // CA证书文件
	CAKeyFile      string              // CA密钥文件
	AutoGenerateCA bool                // 是否自动生成CA
}

// Manager 认证管理器实现
type Manager struct {
	config    *Config
	ctx       context.Context
	cancel    context.CancelFunc
	jwtSecret []byte
}

// JWTClaims JWT 声明结构
type JWTClaims struct {
	NodeID  string `json:"node_id"`
	GroupID string `json:"group_id"`
	jwt.RegisteredClaims
}

// NewManager 创建认证管理器
func NewManager(config *Config) (*Manager, error) {
	// 初始化上下文
	ctx, cancel := context.WithCancel(context.Background())

	// 检查配置
	if config.TokenSecret == "" {
		cancel()
		return nil, fmt.Errorf("token secret is required")
	}

	// 初始化认证管理器
	m := &Manager{
		config:    config,
		ctx:       ctx,
		cancel:    cancel,
		jwtSecret: []byte(config.TokenSecret),
	}

	return m, nil
}

// Start 启动认证管理器
func (m *Manager) Start(ctx context.Context) error {
	// 认证管理器已经在NewManager中初始化
	return nil
}

// Stop 停止认证管理器
func (m *Manager) Stop() error {
	// 取消上下文
	m.cancel()

	return nil
}

// Authenticate 认证
func (m *Manager) Authenticate(ctx context.Context, token string) (bool, error) {
	// 验证令牌
	authToken, err := m.ValidateToken(ctx, token)
	if err != nil {
		return false, err
	}

	// 检查令牌是否过期
	if authToken.ExpiresAt.Before(time.Now()) {
		return false, fmt.Errorf("token expired")
	}

	return true, nil
}

// GenerateToken 生成认证令牌（JWT）
func (m *Manager) GenerateToken(ctx context.Context, nodeID datasync.NodeID, groupID datasync.GroupID) (string, error) {
	now := time.Now()
	expiresAt := now.Add(m.config.TokenTTL)
	if m.config.TokenTTL == 0 {
		// 默认24小时
		expiresAt = now.Add(24 * time.Hour)
	}

	claims := JWTClaims{
		NodeID:  string(nodeID),
		GroupID: string(groupID),
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			Subject:   string(nodeID),
			Issuer:    "datasync",
			Audience:  []string{string(groupID)},
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(m.jwtSecret)
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	return tokenString, nil
}

// ValidateToken 验证令牌（JWT）
func (m *Manager) ValidateToken(ctx context.Context, tokenString string) (*datasync.AuthToken, error) {
	if tokenString == "" {
		return nil, fmt.Errorf("token is empty")
	}

	token, err := jwt.ParseWithClaims(tokenString, &JWTClaims{}, func(token *jwt.Token) (interface{}, error) {
		// 验证签名方法
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return m.jwtSecret, nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	if !token.Valid {
		return nil, fmt.Errorf("token is invalid")
	}

	claims, ok := token.Claims.(*JWTClaims)
	if !ok {
		return nil, fmt.Errorf("invalid token claims")
	}

	return &datasync.AuthToken{
		NodeID:    datasync.NodeID(claims.NodeID),
		GroupID:   datasync.GroupID(claims.GroupID),
		ExpiresAt: claims.ExpiresAt.Time,
		IssuedAt:  claims.IssuedAt.Time,
	}, nil
}

// RefreshToken 刷新令牌
func (m *Manager) RefreshToken(ctx context.Context, tokenString string) (string, error) {
	// 验证旧令牌
	authToken, err := m.ValidateToken(ctx, tokenString)
	if err != nil {
		return "", fmt.Errorf("invalid token: %w", err)
	}

	// 检查令牌是否即将过期（例如，剩余时间小于1小时）
	if authToken.ExpiresAt.After(time.Now().Add(1 * time.Hour)) {
		return "", fmt.Errorf("token is not close to expiration")
	}

	// 生成新令牌
	return m.GenerateToken(ctx, authToken.NodeID, authToken.GroupID)
}
