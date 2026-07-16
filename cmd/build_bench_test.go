// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package cmd

import (
	"context"
	"runtime"
	"testing"

	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// BenchmarkBuild_Default measures the per-Build cost for the default
// configuration (service commands + shortcuts + plugins + strict mode).
// This is the hot-path baseline for repeated Build invocations.
func BenchmarkBuild_Default(b *testing.B) {
	// Warm one-time caches first
	_ = Build(context.Background(), cmdutil.InvocationContext{})
	runtime.GC()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Build(context.Background(), cmdutil.InvocationContext{})
	}
}

// BenchmarkBuild_WithoutServiceCommands measures the Build cost without
// service command registration. The delta from Default gives the
// service-command registration cost.
func BenchmarkBuild_WithoutServiceCommands(b *testing.B) {
	_ = Build(context.Background(), cmdutil.InvocationContext{}, WithoutServiceCommands())
	runtime.GC()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Build(context.Background(), cmdutil.InvocationContext{}, WithoutServiceCommands())
	}
}

// BenchmarkBuild_WithoutPlugins measures the Build cost without plugins.
// The delta from Default gives the plugin + policy + hook cost.
func BenchmarkBuild_WithoutPlugins(b *testing.B) {
	_ = Build(context.Background(), cmdutil.InvocationContext{}, WithoutPlugins())
	runtime.GC()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Build(context.Background(), cmdutil.InvocationContext{}, WithoutPlugins())
	}
}

// BenchmarkBuild_WithoutServiceAndPlugins measures the Build cost with
// neither service commands nor plugins. This isolates the base cost
// (root command + builtins + shortcuts).
func BenchmarkBuild_WithoutServiceAndPlugins(b *testing.B) {
	_ = Build(context.Background(), cmdutil.InvocationContext{}, WithoutServiceCommands(), WithoutPlugins())
	runtime.GC()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Build(context.Background(), cmdutil.InvocationContext{}, WithoutServiceCommands(), WithoutPlugins())
	}
}

// TestBuild_CommandTreeStats counts the total number of commands,
// runnable commands, and flags in the default build. This gives us
// the scale of the command tree to reason about optimization targets.
func TestBuild_CommandTreeStats(t *testing.T) {
	root := Build(context.Background(), cmdutil.InvocationContext{}, WithoutPlugins())

	var totalCmds, runnableCmds, groupCmds int
	var totalFlags int

	var walk func(cmd *cobra.Command)
	walk = func(cmd *cobra.Command) {
		totalCmds++
		if cmd.RunE != nil || cmd.Run != nil {
			runnableCmds++
		} else {
			groupCmds++
		}
		if cmd.Flags() != nil {
			cmd.Flags().VisitAll(func(f *pflag.Flag) {
				totalFlags++
			})
		}
		for _, child := range cmd.Commands() {
			walk(child)
		}
	}
	walk(root)

	t.Logf("Command tree stats:")
	t.Logf("  Total commands: %d", totalCmds)
	t.Logf("  Runnable commands: %d", runnableCmds)
	t.Logf("  Group commands: %d", groupCmds)
	t.Logf("  Total flags: %d", totalFlags)
}
