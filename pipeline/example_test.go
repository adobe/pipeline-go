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

package pipeline_test

import (
	"context"
	"fmt"
	"github.com/adobe/pipeline-go/pipeline"
	"github.com/adobe/ims-go/ims"
	"log"
	"os"
	"time"
)

func Example_authentication() {
	var (
		imsURL          = os.Getenv("IMS_URL")
		imsCode         = os.Getenv("IMS_CODE")
		imsClientID     = os.Getenv("IMS_CLIENT_ID")
		imsClientSecret = os.Getenv("IMS_CLIENT_SECRET")
		pipelineURL     = os.Getenv("PIPELINE_URL")
		pipelineGroup   = os.Getenv("PIPELINE_URL")
	)

	// Create an IMS client.

	imsClient, err := ims.NewClient(&ims.ClientConfig{
		URL: imsURL,
	})
	if err != nil {
		log.Fatalf("error: create IMS client: %v", err)
	}

	// Create a TokenGetter based on the IMS client.

	tokenGetter := pipeline.TokenGetterFunc(func(ctx context.Context) (string, error) {
		res, err := imsClient.Token(&ims.TokenRequest{
			Code:         imsCode,
			ClientID:     imsClientID,
			ClientSecret: imsClientSecret,
		})
		if err != nil {
			return "", fmt.Errorf("read token: %v", err)
		}
		return res.AccessToken, nil
	})

	// Create a Pipeline client.

	_, err = pipeline.NewClient(&pipeline.ClientConfig{
		PipelineURL: pipelineURL,
		Group:       pipelineGroup,
		TokenGetter: tokenGetter,
	})
	if err != nil {
		log.Fatalf("error: create Pipeline client: %v", err)
	}
}

func Example_receive() {
	var (
		pipelineURL         = os.Getenv("PIPELINE_URL")
		pipelineGroup       = os.Getenv("PIPELINE_GROUP")
		pipelineToken       = os.Getenv("PIPELINE_TOKEN")
		pipelineTopic       = os.Getenv("PIPELINE_TOPIC")
		receiveOrganization = os.Getenv("RECEIVE_ORGANIZATION")
		receiveSource       = os.Getenv("RECEIVE_SOURCE")
	)

	// Create a TokenGetter.

	tokenGetter := pipeline.TokenGetterFunc(func(ctx context.Context) (string, error) {
		return pipelineToken, nil
	})

	// Create a Pipeline client.

	client, err := pipeline.NewClient(&pipeline.ClientConfig{
		PipelineURL: pipelineURL,
		Group:       pipelineGroup,
		TokenGetter: tokenGetter,
	})
	if err != nil {
		log.Fatalf("error: create client: %v", err)
	}

	// Consume messages from Pipeline.

	ch := client.Receive(context.Background(), pipelineTopic, &pipeline.ReceiveRequest{
		Organizations:     []string{receiveOrganization},
		Sources:           []string{receiveSource},
		ReconnectionDelay: 1 * time.Minute,
		PingTimeout:       5 * time.Minute,
	})

	for msg := range ch {
		switch {
		case msg.Err != nil:
			log.Println("error:", msg.Err)
		default:
			log.Println("message received:", msg.Envelope.Type)
		}
	}
}

func Example_send() {
	var (
		pipelineURL   = os.Getenv("PIPELINE_URL")
		pipelineGroup = os.Getenv("PIPELINE_GROUP")
		pipelineToken = os.Getenv("PIPELINE_TOKEN")
		pipelineTopic = os.Getenv("PIPELINE_TOPIC")
	)

	// Create a TokenGetter.

	tokenGetter := pipeline.TokenGetterFunc(func(ctx context.Context) (string, error) {
		return pipelineToken, nil
	})

	// Create a Pipeline client.

	client, err := pipeline.NewClient(&pipeline.ClientConfig{
		PipelineURL: pipelineURL,
		Group:       pipelineGroup,
		TokenGetter: tokenGetter,
	})
	if err != nil {
		log.Fatalf("error: create client: %v", err)
	}

	// Send a message over the Pipeline to the VA6 and VA7 locations.

	err = client.Send(context.Background(), pipelineTopic, &pipeline.SendRequest{
		Messages: []pipeline.Message{
			{
				Value:     []byte(`"this is a test message"`),
				Locations: []string{"VA6", "VA7"},
			},
		},
	})
	if err != nil {
		log.Fatalf("error: send message: %v", err)
	}
}

func Example_sync() {
	var (
		pipelineURL         = os.Getenv("PIPELINE_URL")
		pipelineGroup       = os.Getenv("PIPELINE_GROUP")
		pipelineToken       = os.Getenv("PIPELINE_TOKEN")
		pipelineTopic       = os.Getenv("PIPELINE_TOPIC")
		receiveOrganization = os.Getenv("RECEIVE_ORGANIZATION")
		receiveSource       = os.Getenv("RECEIVE_SOURCE")
	)

	// Create a TokenGetter.

	tokenGetter := pipeline.TokenGetterFunc(func(ctx context.Context) (string, error) {
		return pipelineToken, nil
	})

	// Create a Pipeline client.

	client, err := pipeline.NewClient(&pipeline.ClientConfig{
		PipelineURL: pipelineURL,
		Group:       pipelineGroup,
		TokenGetter: tokenGetter,
	})
	if err != nil {
		log.Fatalf("error: create client: %v", err)
	}

	// Consume messages from Pipeline. Note the use of the SyncInterval in the
	// ReceiveRequest, which instructs the Pipeline API to send SYNC envelopes
	// with a sync marker in it.

	ctx := context.Background()

	ch := client.Receive(ctx, pipelineTopic, &pipeline.ReceiveRequest{
		Organizations:     []string{receiveOrganization},
		Sources:           []string{receiveSource},
		ReconnectionDelay: 1 * time.Minute,
		PingTimeout:       5 * time.Minute,
		SyncInterval:      10 * time.Second,
	})

	// While processing envelopes, send the sync marker found in SYNC envelopes
	// back to the Pipeline API.

	for msg := range ch {
		switch {
		case msg.Err != nil:
			log.Println("error:", msg.Err)
		case msg.Envelope.Type == "SYNC":
			if err := client.Sync(ctx, msg.Envelope.SyncMarker); err != nil {
				log.Println("sync error:", err)
			}
		default:
			log.Println("message received:", msg.Envelope.Type)
		}
	}
}
