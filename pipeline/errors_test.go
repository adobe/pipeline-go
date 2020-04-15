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
	"github.com/google/go-cmp/cmp"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
)

func TestNewError(t *testing.T) {
	res := &http.Response{
		StatusCode: http.StatusInternalServerError,
		Body: ioutil.NopCloser(strings.NewReader(`
			{
				"status": 1,
				"title": "title"
			}
		`)),
	}

	got := newError(res)

	exp := &Error{
		StatusCode: http.StatusInternalServerError,
		Status:     1,
		Title:      "title",
	}

	if err, ok := got.(*Error); !ok {
		t.Fatalf("expected an Error")
	} else if !cmp.Equal(exp, err) {
		t.Fatalf("invalid error:\n%v", cmp.Diff(exp, err))
	} else if err.Error() != err.Title {
		t.Fatalf("invalid error message: %v", err)
	}
}

func TestNewErrorParseError(t *testing.T) {
	res := &http.Response{
		StatusCode: http.StatusInternalServerError,
		Body:       ioutil.NopCloser(strings.NewReader("invalid")),
	}

	err := newError(res)

	if err == nil {
		t.Fatalf("expected error")
	} else if _, ok := err.(*Error); ok {
		t.Fatalf("an Error was returned")
	} else if !strings.HasPrefix(err.Error(), "decode response:") {
		t.Fatalf("invalid error: %v", err)
	}
}
