// Package config loads runtime configuration from a YAML file with
// environment variable overrides, following the twelve-factor pattern
// used across the server and worker binaries.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Postgres PostgresConfig `yaml:"postgres"`
	Redis    RedisConfig    `yaml:"redis"`
	Lease    LeaseConfig    `yaml:"lease"`
	Retry    RetryConfig    `yaml:"retry"`
	Metrics  MetricsConfig  `yaml:"metrics"`
}

type ServerConfig struct {
	GRPCAddr string `yaml:"grpc_addr"`
}

type PostgresConfig struct {
	DSN          string `yaml:"dsn"`
	MaxOpenConns int    `yaml:"max_open_conns"`
}

type RedisConfig struct {
	Addr string `yaml:"addr"`
	DB   int    `yaml:"db"`
}

type LeaseConfig struct {
	TTL time.Duration `yaml:"ttl"`
}

type RetryConfig struct {
	BaseDelay   time.Duration `yaml:"base_delay"`
	MaxDelay    time.Duration `yaml:"max_delay"`
	MaxAttempts int           `yaml:"max_attempts"`
}

type MetricsConfig struct {
	Addr string `yaml:"addr"`
	Path string `yaml:"path"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: reading %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parsing %s: %w", path, err)
	}

	applyEnvOverrides(&cfg)
	return &cfg, nil
}

// applyEnvOverrides lets container deployments (see deployments/docker-compose.yml)
// inject secrets like DSNs without baking them into the config file.
func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("TASKQUEUE_POSTGRES_DSN"); v != "" {
		cfg.Postgres.DSN = v
	}
	if v := os.Getenv("TASKQUEUE_REDIS_ADDR"); v != "" {
		cfg.Redis.Addr = v
	}
}
