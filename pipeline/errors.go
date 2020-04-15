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
	"encoding/json"
	"fmt"
	"net/http"
)

// ReportError is a detailed error returned by Adobe Pipeline.
type ReportError struct {
	// The ID for this error.
	ID string `json:"id"`
	// A code associated to this error.
	Code string `json:"code"`
	// A message associated to this error.
	Message string `json:"message"`
}

// Report is a collection of Adobe Pipeline errors.
type Report struct {
	// Errors is a collection of detailed errors.
	Errors []ReportError `json:"errors"`
}

// Error is an error message whose information is gathered from an error
// response returned by Adobe Pipeline.
type Error struct {
	// The HTTP status code of the response.
	StatusCode int
	// The status of the error response.
	Status int `json:"status"`
	// A human-readable message for the error.
	Title string `json:"title"`
	// A more detailed report of individual errors.
	Report Report `json:"report"`
}

func (e *Error) Error() string {
	return e.Title
}

func newError(res *http.Response) error {
	var e Error

	if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
		return fmt.Errorf("decode response: %v", err)
	}

	e.StatusCode = res.StatusCode

	return &e
}
