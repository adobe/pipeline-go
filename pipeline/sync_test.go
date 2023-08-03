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
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSync(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := r.Method; v != http.MethodPost {
			t.Fatalf("invalid method: %s", v)
		}
		if v := r.URL.Path; v != "/pipeline/consumers/g/sync" {
			t.Fatalf("invalid path: %s", v)
		}
		if v := r.Header.Get("authorization"); v != "Bearer token" {
			t.Fatalf("invalid authorization header: %s", v)
		}
		if data, err := ioutil.ReadAll(r.Body); err != nil {
			t.Fatalf("read request: %v", err)
		} else if s := string(data); s != "marker" {
			t.Fatalf("invalid marker: %s", s)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer s.Close()

	c, err := NewClient(&ClientConfig{
		PipelineURL: s.URL,
		Group:       "g",
		TokenGetter: stringTokenGetter("token"),
	})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	if err := c.Sync(context.Background(), "marker"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSyncError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"title": "nope"}`)
	}))
	defer s.Close()

	retryClient := defaultRetryClient()
	retryClient.RetryWaitMax = 5 * time.Millisecond
	retryClient.RetryMax = 2

	c, err := NewClient(&ClientConfig{
		Client:      retryClient.StandardClient(),
		PipelineURL: s.URL,
		Group:       "g",
		TokenGetter: stringTokenGetter("token"),
	})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	if err := c.Sync(context.Background(), "marker"); err == nil {
		t.Fatalf("expected error: %v", err)
	} else if !strings.Contains(err.Error(), "nope") {
		t.Fatalf("invalid error: %v", err)
	}
}

func TestSyncTokenGetterError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("request sent")
	}))
	defer s.Close()

	c, err := NewClient(&ClientConfig{
		PipelineURL: s.URL,
		Group:       "g",
		TokenGetter: errorTokenGetter("nope"),
	})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	if err := c.Sync(context.Background(), "marker"); err == nil {
		t.Fatalf("expected error: %v", err)
	} else if !strings.Contains(err.Error(), "nope") {
		t.Fatalf("invalid error: %v", err)
	}
}
