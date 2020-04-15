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

func envelopeStream(ctx context.Context, body io.ReadCloser) <-chan EnvelopeOrError {
	out := make(chan EnvelopeOrError)

	go func() {
		defer close(out)
		defer body.Close()

		decoder := json.NewDecoder(body)

		for {
			e := new(Envelope)

			if err := decoder.Decode(e); err != nil {
				if err == io.EOF {
					return
				}

				select {
				case out <- EnvelopeOrError{Err: fmt.Errorf("decode envelope: %v", err)}:
					return
				case <-ctx.Done():
					return
				}
			}

			select {
			case out <- EnvelopeOrError{Envelope: e}:
				continue
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

func pingFilter(ctx context.Context, in <-chan EnvelopeOrError, timeout time.Duration) <-chan EnvelopeOrError {
	out := make(chan EnvelopeOrError)

	go func() {
		defer close(out)

		timer := time.NewTimer(timeout)

		defer func() {
			if timer.Stop() {
				return
			}

			select {
			case <-timer.C:
				// The timer expired but we didn't drain the channel. Now the
				// channel is empty and the timer will be correctly cleaned up.
			default:
				// The timer expired, but the channel was already drained, or
				// the timer has been already closed. We dont't have to do
				// anything else.
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				return
			case msg, ok := <-in:
				if !ok {
					return
				}

				if msg.Envelope != nil && msg.Envelope.Type == "PING" {
					if !timer.Stop() {
						return
					}
					timer.Reset(timeout)
				}

				select {
				case out <- msg:
					continue
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out
}

func endOfStreamFilter(ctx context.Context, in <-chan EnvelopeOrError) <-chan EnvelopeOrError {
	out := make(chan EnvelopeOrError)

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-in:
				if !ok {
					return
				}

				select {
				case out <- msg:
					// Message sent.
				case <-ctx.Done():
					return
				}

				if msg.Envelope != nil && msg.Envelope.Type == "END_OF_STREAM" {
					return
				}
			}
		}
	}()

	return out
}

type streamGetter func(ctx context.Context) (<-chan EnvelopeOrError, error)

func reconnectStream(ctx context.Context, stream streamGetter, delay time.Duration) <-chan EnvelopeOrError {
	out := make(chan EnvelopeOrError)

	go func() {
		defer close(out)

		for {
			stop := func() bool {
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				in, err := stream(streamCtx)

				if err != nil {
					select {
					case out <- EnvelopeOrError{Err: fmt.Errorf("get stream: %v", err)}:
						// Message sent
					case <-ctx.Done():
						return true
					}
					select {
					case <-time.After(delay):
						return false
					case <-ctx.Done():
						return true
					}
				}

				for {
					select {
					case msg, ok := <-in:
						if !ok {
							return false
						}
						select {
						case out <- msg:
							// Message sent
						case <-ctx.Done():
							return true
						}
					case <-ctx.Done():
						return true
					}
				}
			}()

			if stop {
				return
			}
		}
	}()

	return out
}
