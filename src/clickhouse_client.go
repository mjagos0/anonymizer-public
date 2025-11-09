package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const (
	clientTimeout = 10 * time.Second
)

type ClickhouseClient struct {
	client *http.Client
}

func NewClickhouseClient() *ClickhouseClient {
	return &ClickhouseClient{
		client: &http.Client{
			Timeout: clientTimeout,
		},
	}
}

func (chc *ClickhouseClient) executeInsert(query string, contentType string, payload []byte) (int, string, error) {
	params := url.Values{}
	params.Add("query", query)

	reqURL := fmt.Sprintf("%s?%s", clickhouseUrl, params.Encode())
	resp, err := chc.client.Post(reqURL, contentType, bytes.NewReader(payload))
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", err
	}
	return resp.StatusCode, string(respBody), nil
}
