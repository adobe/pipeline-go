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

import "encoding/json"

// Message is a message published by a client or received through the pipeline.
type Message struct {
	// Usually it's the imsOrg for the customer that own the data in the
	// message. Only required if publishing to a routed topic.
	ImsOrg string `json:"imsOrg,omitempty"`
	// The message key is used for partitioning/ordering.
	Key string `json:"key,omitempty"`
	// The pipeline instance where this message should be routed or where this
	// message came from. Only valid for routed topics.
	Locations []string `json:"locations,omitempty"`
	// Identifies the service that generated the message.
	Source string `json:"source,omitempty"`
	// This is the actual JSON message.
	Value json.RawMessage `json:"value"`
}
