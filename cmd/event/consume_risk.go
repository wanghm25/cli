// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"io"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/larksuite/cli/internal/cmdutil"
	eventlib "github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/app"
)

// applyArgAwareConsumeRisk overrides the consume command's conservative static
// "write" risk annotation with the precise per-invocation risk WHENEVER this
// process's arguments identify a resolvable EventKey for `event consume`.
//
// The framework reads a command's risk annotation before its RunE ever parses
// the positional argument — the pre-startup command pruning that a user/plugin
// `max_risk` policy drives walks the whole command tree, and the `--help` "Risk:"
// line is rendered off the same annotation. Both must therefore reflect what THIS
// invocation will actually do: a refined key provisions (creates / reuses /
// reactivates) a remote Subscription before the bus starts — a real write — while
// an ordinary (legacy) key only attaches to event delivery and never writes
// remote management state (a read). A single static tag cannot express that, so
// the command annotates itself statically with the worst case and this refines it
// once the argument is visible on the command line.
//
// The value comes from the SAME policy the use case consults at run time
// (app.InvocationDescriptor.Risk), never a second copy. When the EventKey cannot
// be identified or resolved before startup — no consume token, not exactly one
// positional argument, or an unregistered key — the annotation is LEFT at the
// caller's conservative static worst case ("write"): this only ever DOWNGRADES to
// "read" for a definitely-legacy key, so it can never under-state a write.
func applyArgAwareConsumeRisk(cmd *cobra.Command, rawArgs []string) {
	level, ok := consumeArgAwareRisk(rawArgs)
	if !ok {
		return
	}
	cmdutil.SetRisk(cmd, level)
}

// consumeArgAwareRisk resolves the arg-aware risk for a `event consume`
// invocation from rawArgs (os.Args[1:] in production). ok is false — meaning
// "keep the conservative static fallback" — whenever the EventKey cannot be
// identified or resolved before startup.
func consumeArgAwareRisk(rawArgs []string) (level string, ok bool) {
	key, dryRun, found := consumeInvocationFromArgs(rawArgs)
	if !found {
		return "", false
	}
	resolved, err := eventlib.ResolveEventKey(key)
	if err != nil {
		return "", false
	}
	return app.DescribeConsume(resolved, dryRun).Risk(), true
}

// consumeInvocationFromArgs extracts, from a raw `... event consume ...` command
// line, the single positional EventKey and whether --dry-run is set. ok is false
// unless the args contain an `event consume` subpath followed by EXACTLY ONE
// positional token. 0 positionals (e.g. `event consume --help`) or 2+ (which can
// only happen when an unrecognized flag's value pflag could not consume leaks
// into the positional list) both fail closed, so the caller keeps the
// conservative static risk rather than guess and risk under-stating a write.
func consumeInvocationFromArgs(rawArgs []string) (key string, dryRun bool, ok bool) {
	rest, found := argsAfterConsume(rawArgs)
	if !found {
		return "", false, false
	}

	fs := pflag.NewFlagSet("consume-risk", pflag.ContinueOnError)
	fs.ParseErrorsWhitelist.UnknownFlags = true
	fs.SetOutput(io.Discard)

	// Mirror consume's value-taking flags plus the sole global (--profile) so
	// pflag consumes each flag's value instead of mistaking it for the positional
	// EventKey — the guard against a --jq/--filter value that happens to resemble a
	// key being read as the key. Non-key value types (--max-events int,
	// --timeout duration) are declared as strings here because only value
	// CONSUMPTION matters, not the parsed value. Bool flags
	// (--quiet/--include-resource-data/--help) need no entry: an unknown bare flag
	// never swallows the following token. --dry-run is declared so it is both
	// consumed and read.
	var (
		strSink   string
		arraySink []string
		dryVal    bool
	)
	fs.StringVar(&strSink, "profile", "", "")
	fs.StringArrayVarP(&arraySink, "param", "p", nil, "")
	fs.StringVar(&strSink, "jq", "", "")
	fs.StringVar(&strSink, "output-dir", "", "")
	fs.StringVar(&strSink, "max-events", "", "")
	fs.StringVar(&strSink, "timeout", "", "")
	fs.StringVar(&strSink, "as", "", "")
	fs.StringVar(&strSink, "filter", "", "")
	fs.BoolVar(&dryVal, "dry-run", false, "")

	if err := fs.Parse(rest); err != nil {
		return "", false, false
	}
	positionals := fs.Args()
	if len(positionals) != 1 {
		return "", false, false
	}
	return positionals[0], dryVal, true
}

// argsAfterConsume returns the argument tokens that follow the `event consume`
// subcommand: everything after the first "consume" token that appears after an
// "event" token. Scoping to a "consume" that follows "event" ignores an earlier
// stray "consume" that is only a flag value (e.g. a profile literally named
// "consume"), and returns false for any invocation that is not `event consume` at
// all.
func argsAfterConsume(rawArgs []string) ([]string, bool) {
	eventIdx := -1
	for i, a := range rawArgs {
		if a == "event" {
			eventIdx = i
			break
		}
	}
	if eventIdx < 0 {
		return nil, false
	}
	for i := eventIdx + 1; i < len(rawArgs); i++ {
		if rawArgs[i] == "consume" {
			return rawArgs[i+1:], true
		}
	}
	return nil, false
}
