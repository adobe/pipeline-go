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
	"encoding/json"
	"fmt"
	"io"
	"time"
)

func envelopeStream(parent context.Context, body io.ReadCloser, pingTimeout time.Duration) <-chan EnvelopeOrError {
	out := make(chan EnvelopeOrError)

	go func() {
		defer body.Close()
		defer close(out)

		var (
			envelope      EnvelopeOrError
			envelopeCh    = make(chan EnvelopeOrError)
			envelopeReady = false
		)

		var (
			deadline   time.Time
			deadlineCh = time.After(pingTimeout)
		)

		ctx, cancel := context.WithCancel(parent)
		defer cancel()

		go decodeEnvelopes(ctx, body, envelopeCh)

		for {
			var (
				inCh  chan EnvelopeOrError
				outCh chan EnvelopeOrError
			)

			if envelopeReady {
				outCh = out
			} else {
				inCh = envelopeCh
			}

			select {
			case outCh <- envelope:
				envelopeReady = false

				if envelope.Err != nil {
					return
				}

				if envelope.Envelope.Type == "END_OF_STREAM" {
					return
				}
			case envelope = <-inCh:
				envelopeReady = true

				if envelope.Err == io.EOF {
					return
				}

				if envelope.Envelope != nil && envelope.Envelope.Type == "PING" {
					deadline = time.Now().Add(pingTimeout)
				}
			case <-deadlineCh:
				now := time.Now()

				if deadline.Before(now) {
					return
				}

				deadlineCh = time.After(deadline.Sub(now))
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

func decodeEnvelopes(ctx context.Context, r io.Reader, out chan<- EnvelopeOrError) {
	decoder := json.NewDecoder(r)

	for {
		envelope, err := decodeEnvelope(decoder)

		select {
		case out <- EnvelopeOrError{Envelope: envelope, Err: err}:
			continue
		case <-ctx.Done():
			return
		}
	}
}

func decodeEnvelope(decoder *json.Decoder) (*Envelope, error) {
	var envelope Envelope

	if err := decoder.Decode(&envelope); err != nil {
		return nil, err
	}

	return &envelope, nil
}

type streamGetter func(ctx context.Context) (<-chan EnvelopeOrError, error)

func reconnectStream(ctx context.Context, stream streamGetter, delay time.Duration) <-chan EnvelopeOrError {
	out := make(chan EnvelopeOrError)

	go func() {
		defer close(out)

		for {
			func() {
				in, err := stream(ctx)

				if err != nil {
					select {
					case out <- EnvelopeOrError{Err: fmt.Errorf("get stream: %v", err)}:
						return
					case <-ctx.Done():
						return
					}
				}

				var (
					envelope      EnvelopeOrError
					envelopeReady = false
					open          = true
				)

				for {
					if !open && !envelopeReady {
						return
					}

					var (
						inCh  <-chan EnvelopeOrError
						outCh chan<- EnvelopeOrError
					)

					if envelopeReady {
						outCh = out
					} else if open {
						inCh = in
					}

					select {
					case envelope, open = <-inCh:
						envelopeReady = open
					case outCh <- envelope:
						envelopeReady = false
					case <-ctx.Done():
						return
					}
				}
			}()

			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}
