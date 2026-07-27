// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package model

import (
	"context"
	"encoding/json"
)

// APIClient: identity is opaque so business code can't bypass pre-flight checks.
type APIClient interface {
	CallAPI(ctx context.Context, method, path string, body interface{}) (json.RawMessage, error)
}
