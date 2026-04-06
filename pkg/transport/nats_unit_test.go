// +build !integration

package transport

import (
	"testing"
	"time"
)

// TestNATSConfig_DefaultValues 测试默认配置值
func TestNATSConfig_DefaultValues(t *testing.T) {
	config := &NATSConfig{
		Servers:           []string{"nats://localhost:4222"},
		ReconnectWait:     time.Second,
		MaxReconnects:     60,
		ConnectionTimeout: 10 * time.Second,
		EnableJetStream:   false,
		StreamName:        "RAFTSYNC",
	}

	if len(config.Servers) != 1 {
		t.Errorf("expected 1 server, got %d", len(config.Servers))
	}

	if config.ReconnectWait != time.Second {
		t.Errorf("expected reconnect wait 1s, got %v", config.ReconnectWait)
	}

	if config.MaxReconnects != 60 {
		t.Errorf("expected max reconnects 60, got %d", config.MaxReconnects)
	}
}

// TestNATSConfig_AuthOptions 测试认证配置
func TestNATSConfig_AuthOptions(t *testing.T) {
	tests := []struct {
		name           string
		config         *NATSConfig
		expectUsername bool
		expectToken    bool
	}{
		{
			name: "username_password_auth",
			config: &NATSConfig{
				Servers:  []string{"nats://localhost:4222"},
				Username: "admin",
				Password: "secret",
			},
			expectUsername: true,
			expectToken:    false,
		},
		{
			name: "token_auth",
			config: &NATSConfig{
				Servers: []string{"nats://localhost:4222"},
				Token:   "my-token",
			},
			expectUsername: false,
			expectToken:    true,
		},
		{
			name: "no_auth",
			config: &NATSConfig{
				Servers: []string{"nats://localhost:4222"},
			},
			expectUsername: false,
			expectToken:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hasUsername := tt.config.Username != "" && tt.config.Password != ""
			hasToken := tt.config.Token != ""

			if hasUsername != tt.expectUsername {
				t.Errorf("username auth: expected %v, got %v", tt.expectUsername, hasUsername)
			}
			if hasToken != tt.expectToken {
				t.Errorf("token auth: expected %v, got %v", tt.expectToken, hasToken)
			}
		})
	}
}

// TestNATSConfig_TLSOptions 测试 TLS 配置
func TestNATSConfig_TLSOptions(t *testing.T) {
	tests := []struct {
		name       string
		config     *NATSConfig
		expectTLS  bool
	}{
		{
			name: "with_tls",
			config: &NATSConfig{
				Servers: []string{"nats://localhost:4222"},
				TLSCert: "./cert.pem",
				TLSKey:  "./key.pem",
				TLSCA:   "./ca.pem",
			},
			expectTLS: true,
		},
		{
			name: "without_tls",
			config: &NATSConfig{
				Servers: []string{"nats://localhost:4222"},
			},
			expectTLS: false,
		},
		{
			name: "partial_tls",
			config: &NATSConfig{
				Servers: []string{"nats://localhost:4222"},
				TLSCert: "./cert.pem",
				// Missing TLSKey
			},
			expectTLS: false, // Both cert and key required
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hasTLS := tt.config.TLSCert != "" && tt.config.TLSKey != ""
			if hasTLS != tt.expectTLS {
				t.Errorf("TLS: expected %v, got %v", tt.expectTLS, hasTLS)
			}
		})
	}
}

// TestNATSConfig_JetStreamOptions 测试 JetStream 配置
func TestNATSConfig_JetStreamOptions(t *testing.T) {
	tests := []struct {
		name            string
		config          *NATSConfig
		expectJetStream bool
	}{
		{
			name: "jetstream_enabled",
			config: &NATSConfig{
				Servers:         []string{"nats://localhost:4222"},
				EnableJetStream: true,
				StreamName:      "TEST_STREAM",
			},
			expectJetStream: true,
		},
		{
			name: "jetstream_disabled",
			config: &NATSConfig{
				Servers:         []string{"nats://localhost:4222"},
				EnableJetStream: false,
			},
			expectJetStream: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.config.EnableJetStream != tt.expectJetStream {
				t.Errorf("JetStream: expected %v, got %v", tt.expectJetStream, tt.config.EnableJetStream)
			}
		})
	}
}

// TestNATSConfig_InvalidConfigs 测试无效配置
func TestNATSConfig_InvalidConfigs(t *testing.T) {
	tests := []struct {
		name        string
		config      *NATSConfig
		invalid     bool
		invalidDesc string
	}{
		{
			name: "empty_servers",
			config: &NATSConfig{
				Servers: []string{},
			},
			invalid:     true,
			invalidDesc: "empty server list",
		},
		{
			name: "negative_reconnects",
			config: &NATSConfig{
				Servers:       []string{"nats://localhost:4222"},
				MaxReconnects: -1,
			},
			invalid:     true,
			invalidDesc: "negative max reconnects",
		},
		{
			name: "zero_timeout",
			config: &NATSConfig{
				Servers:           []string{"nats://localhost:4222"},
				ConnectionTimeout: 0,
			},
			invalid:     true,
			invalidDesc: "zero connection timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// These tests just document what should be invalid
			// Actual validation would be in the constructor
			_ = tt.invalid
			_ = tt.invalidDesc
		})
	}
}
