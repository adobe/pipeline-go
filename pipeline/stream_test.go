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
	"github.com/google/go-cmp/cmp"
	"io/ioutil"
	"log"
	"strings"
	"testing"
	"time"
)

func TestEnvelopeStream(t *testing.T) {
	body := ioutil.NopCloser(strings.NewReader(`
		{
			"envelopeType": "type-1",
			"partition": 1,
			"key": "key-1",
			"offset": 2,
			"topic": "topic-1",
			"createTime": 3,
			"pipelineMessage": {
				"imsOrg": "org-1",
				"key": "key-1",
				"locations": ["loc-1", "loc-2"],
				"source": "source-1",
				"value": "value-1"
			},
			"syncMarker": "marker-1"
		}
		{
			"envelopeType": "type-2",
			"partition": 4,
			"key": "key-2",
			"offset": 5,
			"topic": "topic-2",
			"createTime": 6,
			"pipelineMessage": {
				"imsOrg": "org-2",
				"key": "key-2",
				"locations": ["loc-3", "loc-4"],
				"source": "source-2",
				"value": "value-2"
			},
			"syncMarker": "marker-2"
		}
	`))

	ch := envelopeStream(context.Background(), body)

	envelopes := []Envelope{
		{
			Type:       "type-1",
			Partition:  1,
			Key:        "key-1",
			Offset:     2,
			Topic:      "topic-1",
			CreateTime: 3,
			Message: Message{
				ImsOrg:    "org-1",
				Key:       "key-1",
				Locations: []string{"loc-1", "loc-2"},
				Source:    "source-1",
				Value:     []byte(`"value-1"`),
			},
			SyncMarker: "marker-1",
		},
		{
			Type:       "type-2",
			Partition:  4,
			Key:        "key-2",
			Offset:     5,
			Topic:      "topic-2",
			CreateTime: 6,
			Message: Message{
				ImsOrg:    "org-2",
				Key:       "key-2",
				Locations: []string{"loc-3", "loc-4"},
				Source:    "source-2",
				Value:     []byte(`"value-2"`),
			},
			SyncMarker: "marker-2",
		},
	}

	for _, e := range envelopes {
		if msg, ok := <-ch; !ok {
			log.Fatalf("the channel is closed")
		} else if msg.Err != nil {
			t.Fatalf("error: %v", msg.Err)
		} else if !cmp.Equal(msg.Envelope, &e) {
			t.Fatalf("invalid message\n%v", cmp.Diff(msg.Envelope, &e))
		}
	}

	if _, ok := <-ch; ok {
		t.Fatalf("the channel should be closed")
	}
}

func TestReceiveInvalidContent(t *testing.T) {
	body := ioutil.NopCloser(strings.NewReader("invalid"))

	ch := envelopeStream(context.Background(), body)

	if msg, ok := <-ch; !ok {
		log.Fatalf("the channel is closed")
	} else if msg.Err == nil {
		log.Fatalf("expected error")
	} else if _, ok := <-ch; ok {
		t.Fatalf("the channel should be closed")
	}

	if _, ok := <-ch; ok {
		t.Fatalf("the channel should be closed")
	}
}

func messageSender(messages []EnvelopeOrError) <-chan EnvelopeOrError {
	out := make(chan EnvelopeOrError)

	go func() {
		defer close(out)

		for _, msg := range messages {
			out <- msg
		}
	}()

	return out
}

func TestPingFilter(t *testing.T) {
	messages := []EnvelopeOrError{
		{
			Envelope: &Envelope{
				Type: "PING",
			},
		},
		{
			Envelope: &Envelope{
				Type: "DATA",
			},
		},
		{
			Envelope: &Envelope{
				Type: "PING",
			},
		},
	}

	out := pingFilter(context.Background(), messageSender(messages), 1*time.Second)

	for _, msg := range messages {
		if got, ok := <-out; !ok {
			t.Fatalf("the channel is closed")
		} else if !cmp.Equal(msg, got) {
			t.Fatalf("invalid message\n%v", cmp.Diff(msg, got))
		}
	}

	if _, ok := <-out; ok {
		t.Fatalf("the channel should be closed")
	}
}

func TestPingFilterTimeout(t *testing.T) {
	out := pingFilter(context.Background(), make(chan EnvelopeOrError), time.Millisecond)

	if _, ok := <-out; ok {
		t.Fatalf("the channel should be closed")
	}
}

func TestPingFilterTimeoutReset(t *testing.T) {
	in := make(chan EnvelopeOrError)

	go func() {
		defer close(in)

		in <- EnvelopeOrError{
			Envelope: &Envelope{
				Type: "PING",
			},
		}
	}()

	out := pingFilter(context.Background(), in, time.Millisecond)

	if _, ok := <-out; !ok {
		t.Fatalf("a message should be returned")
	}

	if _, ok := <-out; ok {
		t.Fatalf("the channel should be closed")
	}
}

func TestEndOfStreamFilter(t *testing.T) {
	messages := []EnvelopeOrError{
		{
			Envelope: &Envelope{
				Type: "PING",
			},
		},
		{
			Envelope: &Envelope{
				Type: "END_OF_STREAM",
			},
		},
	}

	out := endOfStreamFilter(context.Background(), messageSender(messages))

	for _, msg := range messages {
		if got, ok := <-out; !ok {
			t.Fatalf("the channel is closed")
		} else if !cmp.Equal(msg, got) {
			t.Fatalf("invalid message\n%v", cmp.Diff(msg, got))
		}
	}

	if _, ok := <-out; ok {
		t.Fatalf("the channel should be closed")
	}
}

func TestReconnectStream(t *testing.T) {
	chans := make(chan chan EnvelopeOrError)

	stream := func(ctx context.Context) (<-chan EnvelopeOrError, error) {
		select {
		case ch := <-chans:
			return ch, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := reconnectStream(ctx, stream, 0)

	func() {
		in := make(chan EnvelopeOrError)
		defer close(in)

		chans <- in

		in <- EnvelopeOrError{Envelope: &Envelope{Type: "1"}}

		if msg, ok := <-out; !ok {
			t.Fatalf("channel should be open")
		} else if msg.Envelope.Type != "1" {
			t.Fatalf("invalid message: %v", msg.Envelope.Type)
		}
	}()

	func() {
		in := make(chan EnvelopeOrError)
		defer close(in)

		chans <- in

		in <- EnvelopeOrError{Envelope: &Envelope{Type: "2"}}

		if msg, ok := <-out; !ok {
			t.Fatalf("channel should be open")
		} else if msg.Envelope.Type != "2" {
			t.Fatalf("invalid message: %v", msg.Envelope.Type)
		}
	}()
}

func TestReconnectStreamError(t *testing.T) {
	errs := make(chan error)

	stream := func(ctx context.Context) (<-chan EnvelopeOrError, error) {
		select {
		case err := <-errs:
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := reconnectStream(ctx, stream, 0)

	func() {
		errs <- fmt.Errorf("nope")

		if msg, ok := <-out; !ok {
			t.Fatalf("channel should be open")
		} else if msg.Err.Error() != "get stream: nope" {
			t.Fatalf("invalid error: %v", msg.Err)
		}
	}()
}

func TestReconnectStreamDelay(t *testing.T) {
	var (
		connections = make(chan time.Time, 2)
		errs        = make(chan error)
	)

	stream := func(ctx context.Context) (<-chan EnvelopeOrError, error) {
		now := time.Now()

		select {
		case connections <- now:
			// Signal sent
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		select {
		case err := <-errs:
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := reconnectStream(ctx, stream, time.Millisecond)

	func() {
		errs <- fmt.Errorf("first error")

		if msg, ok := <-out; !ok {
			t.Fatalf("channel should be open")
		} else if !strings.Contains(msg.Err.Error(), "first error") {
			t.Fatalf("invalid error: %v", msg.Err)
		}
	}()

	func() {
		errs <- fmt.Errorf("second error")

		if msg, ok := <-out; !ok {
			t.Fatalf("channel should be open")
		} else if !strings.Contains(msg.Err.Error(), "second error") {
			t.Fatalf("invalid error: %v", msg.Err)
		}
	}()

	first, second := <-connections, <-connections

	if delay := second.Sub(first); delay < time.Millisecond {
		t.Fatalf("invalid delay: %v", delay)
	}
}
