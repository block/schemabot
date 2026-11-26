package changeevents

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"text/template"
)

type httpPublisher struct {
	url    string
	client *http.Client
}

func newHTTPDestination(config Destination) (PublisherDestination, error) {
	parsed, err := url.Parse(config.URL)
	if err != nil || parsed.Hostname() == "" || parsed.User != nil || parsed.Fragment != "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return PublisherDestination{}, fmt.Errorf("expected an HTTP(S) URL without credentials or a fragment")
	}
	if strings.TrimSpace(config.Body) == "" {
		return PublisherDestination{}, fmt.Errorf("body template is empty")
	}
	tmpl, err := template.New("event").Option("missingkey=error").Funcs(template.FuncMap{
		"json": func(value any) (string, error) {
			encoded, err := json.Marshal(value)
			return string(encoded), err
		},
	}).Parse(config.Body)
	if err != nil {
		return PublisherDestination{}, fmt.Errorf("parse body template: %w", err)
	}
	return PublisherDestination{
		Events:       config.Events,
		Environments: config.Environments,
		Encode: func(event Event) (json.RawMessage, error) {
			var body bytes.Buffer
			err := tmpl.Execute(&body, event)
			return body.Bytes(), err
		},
		Publisher: &httpPublisher{url: config.URL, client: &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}},
	}, nil
}

func (p *httpPublisher) Publish(ctx context.Context, payload json.RawMessage) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create change event request")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		// A url.Error includes the configured URL, which can contain a token.
		requestErr := &url.Error{}
		if errors.As(err, &requestErr) {
			err = requestErr.Err
		}
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP status %d", resp.StatusCode)
	}
	return nil
}
