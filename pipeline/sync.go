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
	"net/http"
	"strings"
)

// Sync track the consuming application's last read position for a given topic
// and consumer group.
func (c *Client) Sync(ctx context.Context, marker string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, syncURL(c.pipelineURL, c.group), strings.NewReader(marker))
	if err != nil {
		return fmt.Errorf("create request: %v", err)
	}

	token, err := c.tokenGetter.Token(ctx)
	if err != nil {
		return fmt.Errorf("get token: %v", err)
	}

	req.Header.Set("authorization", fmt.Sprintf("Bearer %s", token))

	res, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("perform request: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusNoContent {
		return newError(res)
	}

	return nil
}

func syncURL(pipelineURL, group string) string {
	u := urlMustParse(pipelineURL)
	u.Path = fmt.Sprintf("/pipeline/consumers/%s/sync", group)
	return u.String()
}
