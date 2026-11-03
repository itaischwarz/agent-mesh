package main

import (
	"errors"
	"fmt"
	"os"

	"sigs.k8s.io/yaml"
)

// Config is the rag-connector config file. Relative paths are resolved
// against the working directory.
type Config struct {
	// StateDir holds per-source sync state (cursors, delivered versions).
	StateDir string `json:"stateDir"`
	// SinkDir receives synced documents.
	SinkDir string   `json:"sinkDir"`
	Sources []Source `json:"sources"`
}

// Source is one thing to sync. Type selects which block below applies.
type Source struct {
	ID         string            `json:"id"`
	Type       string            `json:"type"` // fs | drive | confluence
	FS         *FSSource         `json:"fs,omitempty"`
	Drive      *DriveSource      `json:"drive,omitempty"`
	Confluence *ConfluenceSource `json:"confluence,omitempty"`
}

type FSSource struct {
	Root string `json:"root"`
}

type DriveSource struct {
	FolderID        string `json:"folderId"`
	CredentialsFile string `json:"credentialsFile,omitempty"`
}

type ConfluenceSource struct {
	BaseURL string `json:"baseURL"`
	Space   string `json:"space"`
	Email   string `json:"email"`
	// TokenEnv names the environment variable holding the API token, so the
	// token itself never lives in the config file.
	TokenEnv string `json:"tokenEnv"`
}

func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.UnmarshalStrict(b, &cfg); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	if c.StateDir == "" || c.SinkDir == "" {
		return errors.New("stateDir and sinkDir are required")
	}
	if len(c.Sources) == 0 {
		return errors.New("no sources configured")
	}
	seen := map[string]bool{}
	for i, s := range c.Sources {
		if s.ID == "" {
			return fmt.Errorf("sources[%d]: id is required", i)
		}
		if seen[s.ID] {
			return fmt.Errorf("duplicate source id %q", s.ID)
		}
		seen[s.ID] = true
		var ok bool
		switch s.Type {
		case "fs":
			ok = s.FS != nil && s.FS.Root != ""
		case "drive":
			ok = s.Drive != nil && s.Drive.FolderID != ""
		case "confluence":
			ok = s.Confluence != nil && s.Confluence.BaseURL != "" && s.Confluence.Space != "" &&
				s.Confluence.Email != "" && s.Confluence.TokenEnv != ""
		default:
			return fmt.Errorf("source %q: unknown type %q (want fs, drive, or confluence)", s.ID, s.Type)
		}
		if !ok {
			return fmt.Errorf("source %q: missing required %s settings", s.ID, s.Type)
		}
	}
	return nil
}
