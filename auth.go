package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"
)

func getOAuth2Client(ctx context.Context, credPath, tokPath string) (*http.Client, error) {
	b, err := os.ReadFile(credPath)
	if err != nil {
		return nil, fmt.Errorf("reading credentials.json: %w\nDownload it from https://console.cloud.google.com/apis/credentials", err)
	}

	config, err := google.ConfigFromJSON(b, gmail.GmailReadonlyScope)
	if err != nil {
		return nil, fmt.Errorf("parsing credentials.json: %w", err)
	}

	tok, err := loadToken(tokPath)
	if err != nil {
		tok, err = getTokenFromWeb(ctx, config)
		if err != nil {
			return nil, err
		}
		if err := saveToken(tokPath, tok); err != nil {
			return nil, err
		}
	}

	return config.Client(ctx, tok), nil
}

func loadToken(path string) (*oauth2.Token, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer func() { _ = f.Close() }()
	var tok oauth2.Token
	if err := json.NewDecoder(f).Decode(&tok); err != nil {
		return nil, err
	}
	return &tok, nil
}

func getTokenFromWeb(ctx context.Context, config *oauth2.Config) (*oauth2.Token, error) {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Open the following URL in your browser and authorize the application:\n\n%s\n\nPaste the authorization code here: ", authURL)

	var code string
	if _, err := fmt.Scan(&code); err != nil {
		return nil, fmt.Errorf("reading authorization code: %w", err)
	}

	tok, err := config.Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("exchanging authorization code: %w", err)
	}
	return tok, nil
}

func saveToken(path string, tok *oauth2.Token) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("saving token: %w", err)
	}

	defer func() { _ = f.Close() }()
	return json.NewEncoder(f).Encode(tok)
}
