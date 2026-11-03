package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExampleConfigLoads(t *testing.T) {
	cfg, err := loadConfig("example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Sources) != 1 || cfg.Sources[0].Type != "fs" {
		t.Fatalf("sources = %+v", cfg.Sources)
	}
}

func TestConfigValidation(t *testing.T) {
	cases := map[string]string{
		"unknown field": "stateDir: s\nsinkDir: k\nsources: [{id: a, type: fs, fs: {root: .}, typo: 1}]",
		"duplicate id":  "stateDir: s\nsinkDir: k\nsources: [{id: a, type: fs, fs: {root: .}}, {id: a, type: fs, fs: {root: .}}]",
		"unknown type":  "stateDir: s\nsinkDir: k\nsources: [{id: a, type: s3}]",
		"missing block": "stateDir: s\nsinkDir: k\nsources: [{id: a, type: drive}]",
		"no dirs":       "sources: [{id: a, type: fs, fs: {root: .}}]",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "c.yaml")
			if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := loadConfig(p); err == nil || strings.Contains(err.Error(), "no such file") {
				t.Fatalf("want validation error, got %v", err)
			}
		})
	}
}
