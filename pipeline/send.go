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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type SendRequest struct {
	Messages []Message `json:"messages"`
}

func (c *Client) Send(ctx context.Context, topic string, sendRequest *SendRequest) error {
	var body bytes.Buffer

	if err := json.NewEncoder(&body).Encode(sendRequest); err != nil {
		return fmt.Errorf("encode request body: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sendURL(c.pipelineURL, topic), &body)
	if err != nil {
		return fmt.Errorf("create request: %v", err)
	}

	req.Header.Set("Content-type", "application/vnd.pipe.json.v1+json")
	req.Header.Set("Connection", "Keep-Alive")
	req.Header.Set("Accept", "application/json")

	token, err := c.tokenGetter.Token(ctx)
	if err != nil {
		return fmt.Errorf("get authorization token: %v", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	res, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("perform request: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return newError(res)
	}

	return nil
}

func sendURL(pipelineURL, topic string) string {
	u := urlMustParse(pipelineURL)
	u.Path = fmt.Sprintf("/pipeline/topics/%s/messages", topic)
	return u.String()
}
