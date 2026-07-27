// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package whiteboard

import (
	"context"
	"strings"
	"testing"
	"time"

	clie2e "github.com/larksuite/cli/tests/cli_e2e"
	"github.com/stretchr/testify/require"
)

func TestWhiteboardNodeUpdateDryRun_RequestShape(t *testing.T) {
	setWhiteboardDryRunEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{
			"whiteboard", "+node-update",
			"--whiteboard-token", "wbcnUpdateDryRun",
			"--source", `{"nodes":[{"id":"nodeA","type":"text","text":{"content":"hello A"}},{"id":"nodeB","type":"text","text":{"content":"hello B"}}]}`,
			"--dry-run",
		},
		DefaultAs: "bot",
	})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)

	out := result.Stdout
	require.Equal(t, int64(2), clie2e.DryRunGet(out, "api.#").Int(), out)
	for i, nodeID := range []string{"nodeA", "nodeB"} {
		require.Equal(t, "PUT", clie2e.DryRunGet(out, "api."+string(rune('0'+i))+".method").String(), out)
		gotURL := clie2e.DryRunGet(out, "api."+string(rune('0'+i))+".url").String()
		if !strings.HasPrefix(gotURL, "/open-apis/board/v1/whiteboards/") ||
			!strings.HasSuffix(gotURL, "/nodes/"+nodeID) ||
			strings.Contains(gotURL, "wbcnUpdateDryRun") {
			t.Fatalf("url=%q, want masked board whiteboard node update URL ending with %s\nstdout:\n%s", gotURL, nodeID, out)
		}
		require.False(t, clie2e.DryRunGet(out, "api."+string(rune('0'+i))+".body.node.id").Exists(), out)
		require.Equal(t, "text", clie2e.DryRunGet(out, "api."+string(rune('0'+i))+".body.node.type").String(), out)
		require.Equal(t, "hello "+string(rune('A'+i)), clie2e.DryRunGet(out, "api."+string(rune('0'+i))+".body.node.text.content").String(), out)
	}
}
