// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package credential

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/larksuite/cli/internal/core"
)

type userInfo struct {
	OpenID string
	Name   string
}

// fetchUserInfo calls /open-apis/authen/v1/user_info with a UAT to get the user's identity.
func fetchUserInfo(ctx context.Context, httpClient *http.Client, brand core.LarkBrand, uat string) (*userInfo, error) {
	ep := core.ResolveEndpoints(brand)
	url := ep.Open + "/open-apis/authen/v1/user_info"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+uat)

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("user_info API returned HTTP %d", resp.StatusCode)
	}

	var result struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			OpenID string `json:"open_id"`
			Name   string `json:"name"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if result.Code != 0 {
		return nil, fmt.Errorf("user_info API error: [%d] %s", result.Code, result.Msg)
	}
	return &userInfo{OpenID: result.Data.OpenID, Name: result.Data.Name}, nil
}

// VerifyUATOpenID positively resolves which user a user access token belongs to
// by calling user_info, returning that token's own open_id. Callers use it to
// fail closed when a resolved UAT cannot be shown to belong to the user they
// expected (for example after the active profile/user switched). It uses the
// same HTTP client the rest of credential resolution does, so proxy/TLS
// configuration is honored, and it never returns or logs the token itself.
func (p *CredentialProvider) VerifyUATOpenID(ctx context.Context, brand core.LarkBrand, uat string) (string, error) {
	if p == nil || p.httpClient == nil {
		return "", fmt.Errorf("credential: no HTTP client configured to verify a user access token")
	}
	hc, err := p.httpClient()
	if err != nil {
		return "", err
	}
	info, err := fetchUserInfo(ctx, hc, brand, uat)
	if err != nil {
		return "", err
	}
	return info.OpenID, nil
}
