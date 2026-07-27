// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package common

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/larksuite/cli/errs"
	"github.com/spf13/cobra"

	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/imcontract"
	"github.com/larksuite/cli/internal/output"
)

// TestOutPartialFailure pins the batch / multi-status contract: the result
// rides on stdout as an ok:false envelope (carrying the full payload), and the
// returned error is the typed partial-failure exit signal (ExitAPI), distinct
// from ErrBare (the silent-exit signal).
func TestOutPartialFailure(t *testing.T) {
	cfg := &core.CliConfig{Brand: core.BrandFeishu, AppID: "cli_x"}
	f, stdout, _, _ := cmdutil.TestFactory(t, cfg)
	rt := TestNewRuntimeContextForAPI(context.Background(), &cobra.Command{Use: "+push"}, cfg, f, core.AsUser)

	payload := map[string]interface{}{
		"summary": map[string]interface{}{"uploaded": 1, "failed": 1},
		"items": []map[string]interface{}{
			{"rel_path": "a.txt", "action": "uploaded"},
			{"rel_path": "b.txt", "action": "failed", "error": "boom"},
		},
	}

	err := rt.OutPartialFailure(payload, nil)

	// 1) typed partial-failure exit signal
	var pfErr *output.PartialFailureError
	if !errors.As(err, &pfErr) {
		t.Fatalf("expected *output.PartialFailureError, got %T: %v", err, err)
	}
	if pfErr.Code != output.ExitAPI {
		t.Errorf("exit code = %d, want %d (ExitAPI)", pfErr.Code, output.ExitAPI)
	}

	// 2) stdout envelope reports ok:false but still carries the full payload
	// (both the succeeded and failed items) — consistent with a success Out().
	var env struct {
		OK   bool                   `json:"ok"`
		Data map[string]interface{} `json:"data"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		t.Fatalf("unmarshal stdout envelope: %v\nstdout: %s", err, stdout.String())
	}
	if env.OK {
		t.Errorf("ok must be false on partial failure, got ok:true\nstdout: %s", stdout.String())
	}
	items, _ := env.Data["items"].([]interface{})
	if len(items) != 2 {
		t.Fatalf("both succeeded and failed items must ride on stdout, got %d items\nstdout: %s", len(items), stdout.String())
	}
}

func TestIMContractRequiredResultStopsFalseSuccess(t *testing.T) {
	cfg := &core.CliConfig{Brand: core.BrandFeishu, AppID: "cli_x"}
	f, stdout, _, _ := cmdutil.TestFactory(t, cfg)
	rt := TestNewRuntimeContextForAPI(context.Background(), &cobra.Command{Use: "+messages-send"}, cfg, f, core.AsUser)
	contract, _ := imcontract.Lookup("im +messages-send")
	rt.contractSession = imcontract.NewSession(contract)

	rt.Out(map[string]any{"message_id": ""}, nil)

	if stdout.Len() != 0 {
		t.Fatalf("false success reached stdout: %s", stdout.String())
	}
	if output.ExitCodeOf(rt.outputErr) != output.ExitInternal {
		t.Fatalf("exit = %d, want 5; err=%v", output.ExitCodeOf(rt.outputErr), rt.outputErr)
	}
}

func TestIMContractPartialWritesOneResultEnvelope(t *testing.T) {
	cfg := &core.CliConfig{Brand: core.BrandFeishu, AppID: "cli_x"}
	f, stdout, _, _ := cmdutil.TestFactory(t, cfg)
	rt := TestNewRuntimeContextForAPI(context.Background(), &cobra.Command{Use: "urgent_app"}, cfg, f, core.AsBot)
	contract, _ := imcontract.Lookup("im messages urgent_app")
	rt.contractSession = imcontract.NewSession(contract)
	rt.contractSession.ObserveRequest(map[string]any{"user_id_list": []any{"ou_a", "ou_b"}})

	rt.Out(map[string]any{"invalid_user_id_list": []any{"ou_b"}}, nil)

	var env map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env["ok"] != false || env["hint"] == "" {
		t.Fatalf("unexpected envelope: %#v", env)
	}
	var partial *output.PartialFailureError
	if !errors.As(rt.outputErr, &partial) || partial.Code != output.ExitAPI {
		t.Fatalf("output error = %T %v", rt.outputErr, rt.outputErr)
	}
}

func TestIMContractFlagCancelPendingLayerIsPartial(t *testing.T) {
	cfg := &core.CliConfig{Brand: core.BrandFeishu, AppID: "cli_x"}
	f, stdout, _, _ := cmdutil.TestFactory(t, cfg)
	rt := TestNewRuntimeContextForAPI(context.Background(), &cobra.Command{Use: "+flag-cancel"}, cfg, f, core.AsUser)
	contract, _ := imcontract.Lookup("im +flag-cancel")
	rt.contractSession = imcontract.NewSession(contract)
	rt.RecordContractFact(imcontract.Fact{Kind: imcontract.FactFlagFeedLayerPending})

	rt.Out(map[string]any{"results": []any{
		map[string]any{"flag_type": "message", "status": "ok"},
	}}, nil)

	var env struct {
		OK   bool `json:"ok"`
		Data struct {
			Completion imcontract.Completion `json:"completion"`
		} `json:"data"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.OK || env.Data.Completion.PendingCount != 1 ||
		len(env.Data.Completion.PendingItems) != 1 || env.Data.Completion.PendingItems[0] != "feed" {
		t.Fatalf("unexpected pending ledger: %#v", env)
	}
}

func TestRunShortcutAppliesIMReplayPolicy(t *testing.T) {
	cfg := &core.CliConfig{Brand: core.BrandFeishu, AppID: "cli_x", AppSecret: "secret"}
	f, _, _, _ := cmdutil.TestFactory(t, cfg)
	parent := &cobra.Command{Use: "im"}
	shortcut := Shortcut{
		Service:     "im",
		Command:     "+flag-create",
		Description: "test",
		Risk:        "write",
		AuthTypes:   []string{"bot"},
		Execute: func(_ context.Context, runtime *RuntimeContext) error {
			runtime.RecordContractFact(imcontract.Fact{Kind: imcontract.FactWriteAttempted})
			return errs.NewNetworkError(errs.SubtypeNetworkTransport, "request failed").WithRetryable()
		},
	}
	shortcut.Mount(parent, f)
	parent.SetArgs([]string{"+flag-create", "--as", "bot"})

	err := parent.Execute()
	p, ok := errs.ProblemOf(err)
	if !ok {
		t.Fatalf("expected typed error, got %T %v", err, err)
	}
	if p.Retryable || p.Hint != "The write result is unknown. Do not replay the original request." {
		t.Fatalf("problem = %#v", p)
	}
}

func TestIMContractAlsoAppliesToPrettyOutput(t *testing.T) {
	cfg := &core.CliConfig{Brand: core.BrandFeishu, AppID: "cli_x"}
	f, stdout, _, _ := cmdutil.TestFactory(t, cfg)
	rt := TestNewRuntimeContextForAPI(context.Background(), &cobra.Command{Use: "+chat-create"}, cfg, f, core.AsUser)
	rt.Format = "pretty"
	contract, _ := imcontract.Lookup("im +chat-create")
	rt.contractSession = imcontract.NewSession(contract)

	rt.OutFormat(map[string]any{"chat_id": ""}, nil, func(w io.Writer) {
		fmt.Fprintln(w, "Group created successfully")
	})

	if stdout.Len() != 0 {
		t.Fatalf("false pretty success reached stdout: %s", stdout.String())
	}
	if output.ExitCodeOf(rt.outputErr) != output.ExitInternal {
		t.Fatalf("exit = %d, want 5; err=%v", output.ExitCodeOf(rt.outputErr), rt.outputErr)
	}
}
