package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// RefreshToken exchanges a refresh token for a fresh set of tokens at the
// issuer's token endpoint, using the same public client as login. It returns the
// new ID token, access token, and refresh token. Expiry is the ID token's expiry,
// used to renew the cached bearer credential without another browser round-trip.
// The refresh token is retained if the provider does not rotate it.
// RedirectPort is unused for the refresh grant.
func RefreshToken(ctx context.Context, cfg LoginConfig, refreshToken string) (*LoginResult, error) {
	if cfg.Issuer == "" {
		return nil, errors.New("refresh: issuer is required")
	}
	if cfg.ClientID == "" {
		return nil, errors.New("refresh: client ID is required")
	}
	if refreshToken == "" {
		return nil, errors.New("refresh: refresh token is required")
	}

	provider, err := oidc.NewProvider(ctx, cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("discover OIDC provider %s: %w", cfg.Issuer, err)
	}

	oauthCfg := &oauth2.Config{
		ClientID: cfg.ClientID,
		Endpoint: provider.Endpoint(),
	}
	// TokenSource performs the refresh_token grant on the first Token() call.
	token, err := oauthCfg.TokenSource(ctx, &oauth2.Token{RefreshToken: refreshToken}).Token()
	if err != nil {
		return nil, fmt.Errorf("refresh token at %s: %w", cfg.Issuer, err)
	}

	rawID, ok := token.Extra("id_token").(string)
	if !ok || rawID == "" {
		return nil, errors.New("refresh response did not include an id_token")
	}

	expiry, err := idTokenExpiry(rawID)
	if err != nil {
		return nil, fmt.Errorf("read refresh ID token expiry: %w", err)
	}

	return &LoginResult{
		IDToken:      rawID,
		AccessToken:  token.AccessToken,
		RefreshToken: token.RefreshToken,
		Expiry:       expiry,
	}, nil
}
