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
	"github.com/hashicorp/go-retryablehttp"
	"net/http"
	"net/url"
)

// ClientConfig is the configuration for a Client.
type ClientConfig struct {
	// HTTP client. If not provided it defaults to the default HTTP client.
	Client *http.Client
	// The URL of the Adobe Pipeline endpoint. Mandatory.
	PipelineURL string
	// The consumer group for this client. Mandatory.
	Group string
	// The strategy for getting an authorization token. Mandatory.
	TokenGetter TokenGetter
}

// Client is a client for Adobe Pipeline.
type Client struct {
	client      *http.Client
	pipelineURL string
	group       string
	tokenGetter TokenGetter
}

// TokenGetter is the user-provided logic for obtaining a Bearer token.
type TokenGetter interface {
	Token(ctx context.Context) (string, error)
}

// TokenGetterFunc implements a TokenGetter backed by a function.
type TokenGetterFunc func(ctx context.Context) (string, error)

func (f TokenGetterFunc) Token(ctx context.Context) (string, error) {
	return f(ctx)
}

// NewClient creates a Client given a ClientConfig.
func NewClient(cfg *ClientConfig) (*Client, error) {
	if _, err := url.Parse(cfg.PipelineURL); err != nil {
		return nil, fmt.Errorf("malformed URL: %v", err)
	}

	if cfg.Group == "" {
		return nil, fmt.Errorf("missing group")
	}

	if cfg.TokenGetter == nil {
		return nil, fmt.Errorf("missing token getter")
	}

	client := cfg.Client

	if client == nil {
		client = defaultRetryClient().StandardClient()
	}

	return &Client{
		client:      client,
		pipelineURL: cfg.PipelineURL,
		group:       cfg.Group,
		tokenGetter: cfg.TokenGetter,
	}, nil
}

// Adobe pipeline makes use of status code 429 in combination of the retry-after header
// the default http client does not retry in these requests, hence using a retriable as default instead
func defaultRetryClient() *retryablehttp.Client {
	rc := retryablehttp.NewClient()
	rc.RetryMax = 10
	rc.Logger = nil
	// use the Passthrough handler to propagate the payload from the last error to the caller in a transparent manner
	rc.ErrorHandler = retryablehttp.PassthroughErrorHandler
	return rc
}

func urlMustParse(u string) *url.URL {
	if p, err := url.Parse(u); err != nil {
		panic(err)
	} else {
		return p
	}
}
