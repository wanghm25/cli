// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package apps

import (
	"errors"
	"testing"

	"github.com/larksuite/cli/errs"
)

func TestAppTablesPath_ReusesExistingURL(t *testing.T) {
	if got := appTablesPath("app_x"); got != "/open-apis/spark/v1/apps/app_x/tables" {
		t.Fatalf("appTablesPath = %q (want existing /apps/{id}/tables, not /db/tables)", got)
	}
}

func TestAppTablePath_EncodesSegments(t *testing.T) {
	if got := appTablePath("app_x", "my table"); got != "/open-apis/spark/v1/apps/app_x/tables/my%20table" {
		t.Fatalf("appTablePath = %q", got)
	}
}

func TestAppSQLPath_ReusesExistingURL(t *testing.T) {
	if got := appSQLPath("app_x"); got != "/open-apis/spark/v1/apps/app_x/sql_commands" {
		t.Fatalf("appSQLPath = %q (want /apps/{id}/sql_commands)", got)
	}
}

func TestAppDbEnvCreatePath_NewURL(t *testing.T) {
	// db-env-create 是本期新增接口，URL 走 /db_dev_init（与上面三条复用 URL 不同）。
	if got := appDbEnvCreatePath("app_x"); got != "/open-apis/spark/v1/apps/app_x/db_dev_init" {
		t.Fatalf("appDbEnvCreatePath = %q", got)
	}
}

func TestRequireAppID_BlankRejected(t *testing.T) {
	if _, err := requireAppID("   "); err == nil {
		t.Fatal("expected error for blank app-id")
	}
	got, err := requireAppID("  app_x  ")
	if err != nil || got != "app_x" {
		t.Fatalf("requireAppID trimmed = %q err=%v", got, err)
	}
}

func TestDBDataRequestErrorClassifiesUntypedFailure(t *testing.T) {
	cause := errors.New("dial failed")
	got := dbDataRequestError(cause, "export request failed", "repair export input")

	problem, ok := errs.ProblemOf(got)
	if !ok ||
		problem.Category != errs.CategoryNetwork ||
		problem.Subtype != errs.SubtypeNetworkTransport ||
		problem.Message != "export request failed" ||
		problem.Hint != "repair export input" ||
		!problem.Retryable {
		t.Fatalf("fallback problem = %#v", problem)
	}
	if !errors.Is(got, cause) {
		t.Fatal("fallback lost the original transport cause")
	}
}
