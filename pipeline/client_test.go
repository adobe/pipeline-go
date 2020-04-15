// Copyright 2019 Adobe. All rights reserved.
//
// This file is licensed to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR REPRESENTATIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type tokenGetterFunc func(ctx context.Context) (string, error)

func (g tokenGetterFunc) Token(ctx context.Context) (string, error) {
	return g(ctx)
}

func stringTokenGetter(token string) tokenGetterFunc {
	return func(ctx context.Context) (string, error) {
		return token, nil
	}
}

func errorTokenGetter(msg string) tokenGetterFunc {
	return func(ctx context.Context) (string, error) {
		return "", errors.New(msg)
	}
}

func TestNewClientInvalidURL(t *testing.T) {
	cfg := &ClientConfig{
		PipelineURL: ":",
	}
	if _, err := NewClient(cfg); err == nil {
		t.Fatalf("expected error")
	} else if !strings.Contains(err.Error(), "malformed URL") {
		t.Fatalf("invalid error: %v", err)
	}
}

func TestNewClientMissingGroup(t *testing.T) {
	cfg := &ClientConfig{
		PipelineURL: "www.acme.com",
		Group:       "",
	}
	if _, err := NewClient(cfg); err == nil {
		t.Fatalf("expected error")
	} else if !strings.Contains(err.Error(), "missing group") {
		t.Fatalf("invalid error: %v", err)
	}
}

func TestNewClientMissingTokenGetter(t *testing.T) {
	cfg := &ClientConfig{
		PipelineURL: "www.acme.com",
		Group:       "g",
		TokenGetter: nil,
	}
	if _, err := NewClient(cfg); err == nil {
		t.Fatalf("expected error")
	} else if !strings.Contains(err.Error(), "missing token getter") {
		t.Fatalf("invalid error: %v", err)
	}
}
