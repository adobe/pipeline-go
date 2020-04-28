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
	"io"
	"net/http"
	"strings"
	"time"
)

// ReceiveRequest is the information sent for setting up the reception of
// messages via Adobe Pipeline.
type ReceiveRequest struct {
	// The interval at which the server should send SYNC envelopes. If not
	// specified, SYNC envelopes are not sent. If specified, it has to be
	// greater than or equal to 5s.
	SyncInterval time.Duration
	// If specified, send only messages for these IMS organizations.
	Organizations []string
	// If specified, send only messages from these sources.
	Sources []string
	// Instructs where to read messages from when connecting to the pipeline.
	Reset Reset
	// If the implementation experiences a failure, it will reconnect to the
	// Adobe Pipeline API. If specified, this field controls how long to wait
	// between reconnects. If not specified, it defaults to 10s.
	ReconnectionDelay time.Duration
	// This timeout specifies the timeout between two PING envelopes. If this
	// timeout expires the library will automatically reconnect to Adobe
	// Pipeline. If not specified, it defaults to 90s.
	PingTimeout time.Duration
}

func (r *ReceiveRequest) reconnectionDelay() time.Duration {
	if r.ReconnectionDelay > 0 {
		return r.ReconnectionDelay
	}
	return 5 * time.Second
}

func (r *ReceiveRequest) pingTimeout() time.Duration {
	if r.PingTimeout > 0 {
		return r.PingTimeout
	}
	return 90 * time.Second
}

// Reset indicates where to read messages from when connecting to the pipeline.
type Reset int

const (
	// Read from the earliest marked position still available to the pipeline.
	ResetEarliest = 1
	// Read from the latest marked position still available to the pipeline.
	ResetLatest = 2
)

// EnvelopeOrError is one message sent to the client when reading from the
//pipeline. Only one of this struct field will be non-nil at any given time.
type EnvelopeOrError struct {
	// The envelope read from the pipeline.
	Envelope *Envelope
	// An error occurred while reading from the pipeline. In case of error
	// (i.e. when this field is non-nil) no special care needs to be taken. If
	// necessary, the client will automatically reinitialize the connection to
	// the pipeline.
	Err error
}

// Envelope is the envelope sent from the pipeline.
type Envelope struct {
	// The type of the envelope. Can be DATA, SYNC, PING, or END_OF_STREAM.
	Type string `json:"envelopeType"`
	// The Kafka partition from which the message came. Only relevant for
	// envelopes of type DATA.
	Partition int `json:"partition"`
	// An optional message key that was used for partition assignment.
	Key string `json:"key"`
	// The Kafka offset of the message. Only relevant for envelopes of type
	// DATA.
	Offset int `json:"offset"`
	// The Kafka topic of the message. Only relevant for envelopes of type DATA.
	Topic string `json:"topic"`
	// The time (UTC) the message was placed onto the consumer's stream. This
	// can be used to track how far beyond the connection is running.
	CreateTime uint64 `json:"createTime"`
	// For envelopes of type DATA, the actual message.
	Message Message `json:"pipelineMessage"`
	// Only populated for envelopes of type SYNC.
	SyncMarker string `json:"syncMarker"`
}

// Receive opens a connection to Adobe Pipeline and consumes messages sent to
// the client. This function automatically handles connection failures and
// reconnects to the Adobe Pipeline.
func (c *Client) Receive(ctx context.Context, topic string, r *ReceiveRequest) <-chan EnvelopeOrError {
	stream := func(ctx context.Context) (<-chan EnvelopeOrError, error) {
		body, err := c.receive(ctx, topic, r)
		if err != nil {
			return nil, err
		}
		return envelopeStream(ctx, body, r.pingTimeout()), nil
	}

	return reconnectStream(ctx, stream, r.reconnectionDelay())
}

func (c *Client) receive(ctx context.Context, topic string, r *ReceiveRequest) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, receiveURL(c.pipelineURL, c.group, topic, r), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %v", err)
	}

	req.Header.Set("accept", "application/json")

	token, err := c.tokenGetter.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("get token: %v", err)
	}

	req.Header.Set("authorization", fmt.Sprintf("Bearer %s", token))

	res, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("perform request: %v", err)
	}

	if res.StatusCode != http.StatusOK {
		err := newError(res)

		if err := res.Body.Close(); err != nil {
			return nil, fmt.Errorf("close response body: %v", err)
		}

		return nil, err
	}

	return res.Body, nil
}

func receiveURL(pipelineURL, group, topic string, r *ReceiveRequest) string {
	u := urlMustParse(pipelineURL)
	u.Path = fmt.Sprintf("/pipeline/topics/%s/messages", topic)

	values := u.Query()
	values.Set("group", group)

	if r.SyncInterval != 0 {
		values.Set("syncInterval", fmt.Sprintf("%d", r.SyncInterval.Milliseconds()))
	}

	if r.Organizations != nil {
		values.Set("org", strings.Join(r.Organizations, ","))
	}

	if r.Sources != nil {
		values.Set("source", strings.Join(r.Sources, ","))
	}

	switch r.Reset {
	case ResetEarliest:
		values.Set("reset", "earliest")
	case ResetLatest:
		values.Set("reset", "latest")
	}

	u.RawQuery = values.Encode()

	return u.String()
}
