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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSend(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := r.Method; v != http.MethodPost {
			t.Fatalf("invalid method: %s", v)
		}
		if v := r.URL.Path; v != "/pipeline/topics/t/messages" {
			t.Fatalf("invalid path: %s", v)
		}
		if v := r.Header.Get("authorization"); v != "Bearer token" {
			t.Fatalf("invalid authorization header: %s", v)
		}
		if v := r.Header.Get("content-type"); v != "application/vnd.pipe.json.v1+json" {
			t.Fatalf("invalid content type header: %s", v)
		}
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

	if err := c.Send(context.Background(), "t", &SendRequest{
		Messages: []Message{
			{
				ImsOrg:    "org-1",
				Key:       "key-1",
				Locations: []string{"loc-1", "loc-2"},
				Source:    "source-1",
				Value:     []byte(`"value-1"`),
			},
			{
				ImsOrg:    "org-2",
				Key:       "key-2",
				Locations: []string{"loc-3", "loc-4"},
				Source:    "source-2",
				Value:     []byte(`"value-2"`),
			},
		},
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSendError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"title": "nope"}`)
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

	if err := c.Send(context.Background(), "t", &SendRequest{}); err == nil {
		t.Fatalf("unexpected error: %v", err)
	} else if !strings.Contains(err.Error(), "nope") {
		t.Fatalf("invalid error: %v", err)
	}
}

func TestSendTokenGetterError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("request performed")
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

	if err := c.Send(context.Background(), "t", &SendRequest{}); err == nil {
		t.Fatalf("unexpected error: %v", err)
	} else if !strings.Contains(err.Error(), "nope") {
		t.Fatalf("invalid error: %v", err)
	}
}
