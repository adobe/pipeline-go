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
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestReceiveURL(t *testing.T) {
	u, err := url.Parse(receiveURL("https://www.acme.com", "g", "t", &ReceiveRequest{}))
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}

	if u.Scheme != "https" {
		t.Fatalf("invalid scheme: %v", u.Scheme)
	}
	if u.Host != "www.acme.com" {
		t.Fatalf("invalid host: %v", u.Host)
	}
	if u.Path != "/pipeline/topics/t/messages" {
		t.Fatalf("invalid path: %v", u.Path)
	}
	if v := u.Query().Get("group"); v != "g" {
		t.Fatalf("invalid group: %v", v)
	}
}

func TestReceiveURLWithSyncInterval(t *testing.T) {
	u, err := url.Parse(receiveURL("https://www.acme.com", "g", "t", &ReceiveRequest{
		SyncInterval: 1 * time.Minute,
	}))
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}

	if v := u.Query().Get("syncInterval"); v != "60000" {
		t.Fatalf("invalid sync interval: %v", v)
	}
}

func TestReceiveURLWithOrganizations(t *testing.T) {
	u, err := url.Parse(receiveURL("https://www.acme.com", "g", "t", &ReceiveRequest{
		Organizations: []string{"o1", "o2"},
	}))
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}

	if v := u.Query().Get("org"); v != "o1,o2" {
		t.Fatalf("invalid sync interval: %v", v)
	}
}

func TestReceiveURLWithSources(t *testing.T) {
	u, err := url.Parse(receiveURL("https://www.acme.com", "g", "t", &ReceiveRequest{
		Sources: []string{"s1", "s2"},
	}))
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}

	if v := u.Query().Get("source"); v != "s1,s2" {
		t.Fatalf("invalid sources: %v", v)
	}
}

func TestReceiveURLWithResetEarliest(t *testing.T) {
	u, err := url.Parse(receiveURL("https://www.acme.com", "g", "t", &ReceiveRequest{
		Reset: ResetEarliest,
	}))
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}

	if v := u.Query().Get("reset"); v != "earliest" {
		t.Fatalf("invalid reset: %v", v)
	}
}

func TestReceiveURLWithResetLatest(t *testing.T) {
	u, err := url.Parse(receiveURL("https://www.acme.com", "g", "t", &ReceiveRequest{
		Reset: ResetLatest,
	}))
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}

	if v := u.Query().Get("reset"); v != "latest" {
		t.Fatalf("invalid reset: %v", v)
	}
}

func TestReceive(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := r.Header.Get("authorization"); v != "Bearer token" {
			t.Fatalf("invalid authorization header: %v", v)
		}
		if v := r.Header.Get("accept"); v != "application/json" {
			t.Fatalf("invalid accept header: %v", v)
		}
		fmt.Fprint(w, `{"envelopeType": "PING"}`)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := c.Receive(ctx, "t", &ReceiveRequest{})

	if msg := <-ch; msg.Envelope == nil {
		t.Fatalf("expected an envelope")
	} else if msg.Envelope.Type != "PING" {
		t.Fatalf("invalid envelope type: %v", msg.Envelope.Type)
	}
}

func TestReceiveTokenGetterError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("request performed")
	}))
	defer s.Close()

	c, err := NewClient(&ClientConfig{
		PipelineURL: s.URL,
		Group:       "g",
		TokenGetter: errorTokenGetter("token error"),
	})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := c.Receive(ctx, "t", &ReceiveRequest{})

	if msg := <-ch; msg.Err == nil {
		t.Fatalf("expected an error")
	} else if !strings.Contains(msg.Err.Error(), "token error") {
		t.Fatalf("unexpected error: %v", msg.Err)
	}
}

func TestReceiveError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"title": "error from the server"}`)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := c.Receive(ctx, "t", &ReceiveRequest{})

	if msg := <-ch; msg.Err == nil {
		t.Fatalf("expected an error")
	} else if !strings.Contains(msg.Err.Error(), "error from the server") {
		t.Fatalf("unexpected error: %v", msg.Err)
	}
}
