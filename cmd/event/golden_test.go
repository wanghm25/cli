// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"

	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"

	_ "github.com/larksuite/cli/events" // registers the real catalog: im.message.created_v1 (refined base), im.message.receive_v1 (legacy), approval.instance.status_changed_v4 (legacy w/ params)
)

// updateGolden regenerates the golden files under testdata/golden from the
// current command output instead of comparing against them. Regenerate with:
//
//	go test ./cmd/event/... -run TestGolden_EventCatalog -update
var updateGolden = flag.Bool("update", false, "update golden files under testdata/golden")

// goldenCase pins one CLI invocation's exact stdout against a golden file
// under testdata/golden/<name>.golden.
type goldenCase struct {
	name   string                                  // golden file basename
	newCmd func(f *cmdutil.Factory) *cobra.Command // NewCmdList / NewCmdSchema
	args   []string                                // CLI args, e.g. {"--json"}
}

// TestGolden_EventCatalog locks the current stdout of the pure-local,
// catalog-facing `event list` / `event schema <key>` commands — the surface
// the upcoming Catalog-consolidation refactor (PR1) is most likely to
// regress. Any unintended change to list/schema output fails one of these
// cases; regenerate intentional changes with -update (see runGoldenCase).
//
// Covers: the full `event list` (--json and text, including the REFINED
// column + footer), the one refined-subscription base key currently in the
// catalog (im.message.created_v1 — see
// events/refined/refined_keys_mock.json, explicitly the PR1 "meta-swap"
// target), a representative legacy/filter-unsupported key
// (im.message.receive_v1), and a representative legacy key with Params +
// inline Values (approval.instance.status_changed_v4) so the Params/Values
// table-rendering path also has a regression anchor.
func TestGolden_EventCatalog(t *testing.T) {
	cases := []goldenCase{
		{"list_json", NewCmdList, []string{"--json"}},
		{"list_text", NewCmdList, []string{}},

		{"schema_im_message_created_v1_json", NewCmdSchema, []string{"im.message.created_v1", "--json"}},
		{"schema_im_message_created_v1_text", NewCmdSchema, []string{"im.message.created_v1"}},

		{"schema_im_message_receive_v1_json", NewCmdSchema, []string{"im.message.receive_v1", "--json"}},
		{"schema_im_message_receive_v1_text", NewCmdSchema, []string{"im.message.receive_v1"}},

		{"schema_approval_instance_status_changed_v4_json", NewCmdSchema, []string{"approval.instance.status_changed_v4", "--json"}},
		{"schema_approval_instance_status_changed_v4_text", NewCmdSchema, []string{"approval.instance.status_changed_v4"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runGoldenCase(t, tc)
		})
	}
}

// runGoldenCase builds cmd via tc.newCmd(f) — the exact NewCmdList/NewCmdSchema
// + cmdutil.TestFactory + bytes.Buffer construction the rest of this
// package's tests use (see list_test.go / schema_test.go), executes it
// in-process with tc.args (never shelling out to a built binary), and
// compares its stdout against testdata/golden/<tc.name>.golden byte-for-byte.
//
// event list / event schema are pure-local — they only read the embedded
// EventKey catalog (populated by the blank github.com/larksuite/cli/events
// import above) and touch neither the network nor on-disk config — so
// cmdutil.TestFactory needs no extra stubbing, and output is deterministic
// across runs/machines: internal/event.ListAll sorts by Key, JSON object
// keys are sorted by encoding/json, and nothing on this path reads a
// timestamp or randomness. Determinism is additionally verified operationally
// by running -update twice and diffing testdata (see phase0 report).
func runGoldenCase(t *testing.T, tc goldenCase) {
	t.Helper()

	f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})

	cmd := tc.newCmd(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	args := tc.args
	if args == nil {
		// Never pass a nil slice to SetArgs: cobra treats a nil c.args as
		// "unset" and falls back to parsing `go test`'s own os.Args, which
		// would feed flags like -test.v into NewCmdList/NewCmdSchema.
		args = []string{}
	}
	cmd.SetArgs(args)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("%s: Execute(%v) error = %v", tc.name, tc.args, err)
	}

	got := stdout.Bytes()
	path := filepath.Join("testdata", "golden", tc.name+".golden")

	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", filepath.Dir(path), err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", path, err)
		}
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v (run `go test ./cmd/event/... -run TestGolden_EventCatalog -update` to create it)", path, err)
	}
	if string(got) != string(want) {
		t.Errorf("%s: stdout does not match golden %s\n--- want ---\n%s\n--- got ---\n%s", tc.name, path, want, got)
	}
}
