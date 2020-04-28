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
	"testing"
	"time"
)

func TestEnvelopeStream(t *testing.T) {
	r, w := io.Pipe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := envelopeStream(ctx, r, time.Millisecond)

	// Write a data message.

	fmt.Fprint(w, `{"envelopeType": "DATA"}`)

	// Check that the message is received.

	if msg, ok := <-out; !ok {
		t.Fatalf("the channel should not be closed")
	} else if msg.Err != nil {
		t.Fatalf("unexpected error: %v", msg.Err)
	} else if msg.Envelope.Type != "DATA" {
		t.Fatalf("invalid envelope: %v", msg.Envelope.Type)
	}

	// Close the body.

	w.Close()

	// Check that the channel is closed.

	if _, ok := <-out; ok {
		t.Fatalf("the channel should be closed")
	}
}

func TestEnvelopeStreamInvalidContent(t *testing.T) {
	r, w := io.Pipe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := envelopeStream(ctx, r, time.Millisecond)

	// Write invalid content.

	fmt.Fprint(w, `invalid`)

	// Check that an error is returned.

	if msg, ok := <-out; !ok {
		t.Fatalf("the channel should not be closed")
	} else if msg.Err == nil {
		t.Fatalf("expected error")
	}

	// Check that the channel is closed.

	if _, ok := <-out; ok {
		t.Fatalf("the channel should be closed")
	}

	// Check that the body is closed.

	if _, err := fmt.Fprint(w, "fail"); err != io.ErrClosedPipe {
		t.Fatalf("the body should be closed: %v", err)
	}
}

func TestEnvelopeStreamPingTimeout(t *testing.T) {
	r, w := io.Pipe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := envelopeStream(ctx, r, time.Millisecond)

	// Write a data message.

	fmt.Fprint(w, `{"envelopeType": "DATA"}`)

	// Check that the message is received.

	if msg, ok := <-out; !ok {
		t.Fatalf("the channel should not be closed")
	} else if msg.Err != nil {
		t.Fatalf("unexpected error: %v", msg.Err)
	} else if msg.Envelope.Type != "DATA" {
		t.Fatalf("invalid envelope: %v", msg.Envelope.Type)
	}

	// Write a ping message

	fmt.Fprint(w, `{"envelopeType": "PING"}`)

	// Check that the message is received.

	if msg, ok := <-out; !ok {
		t.Fatalf("the channel should not be closed")
	} else if msg.Err != nil {
		t.Fatalf("unexpected error: %v", msg.Err)
	} else if msg.Envelope.Type != "PING" {
		t.Fatalf("invalid envelope: %v", msg.Envelope.Type)
	}

	// Let the timeout expire and check that the channel is closed.

	if _, ok := <-out; ok {
		t.Fatalf("the channel should be closed")
	}

	// Check that the body is closed.

	if _, err := fmt.Fprint(w, "fail"); err != io.ErrClosedPipe {
		t.Fatalf("the body should have been closed")
	}
}

func TestEnvelopeStreamEndOfStream(t *testing.T) {
	r, w := io.Pipe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := envelopeStream(ctx, r, time.Millisecond)

	// Write an end of stream message.

	fmt.Fprint(w, `{"envelopeType": "END_OF_STREAM"}`)

	// Check that the message is received.

	if msg, ok := <-out; !ok {
		t.Fatalf("the channel should not be closed")
	} else if msg.Err != nil {
		t.Fatalf("unexpected error: %v", msg.Err)
	} else if msg.Envelope.Type != "END_OF_STREAM" {
		t.Fatalf("invalid envelope: %v", msg.Envelope.Type)
	}

	// Let the timeout expire and check that the channel is closed.

	if _, ok := <-out; ok {
		t.Fatalf("the channel should be closed")
	}

	// Check that the body is closed.

	if _, err := fmt.Fprint(w, "fail"); err != io.ErrClosedPipe {
		t.Fatalf("the body should have been closed")
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
