// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import "github.com/larksuite/cli/internal/core"

// CommandContext carries the resolved invocation context needed to RENDER
// re-runnable `lark-cli` command hints so a suggested "next step" / recovery
// command targets the SAME app and identity the hint was produced for: the
// resolved profile (rendered as the global --profile) and the resolved identity
// (rendered as --as).
//
// It is the rendering companion of cmdutil.InvocationContext. The command layer
// builds it from the resolved cfg.ProfileName + the resolved --as identity; the
// consume domain builds it from the same values already threaded through its
// options. The profile/identity is therefore single-sourced from the one
// invocation — this type re-tracks no state, it only renders. It lives in this
// domain-neutral facade rather than cmdutil because the consume domain package
// (internal/event/consume) emits command hints too and must not import the CLI
// util layer.
//
// The zero value renders a bare `lark-cli` with neither flag.
type CommandContext struct {
	// Profile is the resolved profile name (core.AppConfig.ProfileName, which
	// round-trips as a --profile value: FindApp matches by name then app_id). An
	// empty Profile omits --profile — the hint then defaults exactly as the
	// current invocation did.
	Profile string
	// Identity is the resolved identity; the zero value ("") omits --as.
	Identity core.Identity
}

// CLIHead renders the command head that carries the global --profile so a hint
// runs against the same app: "lark-cli" or "lark-cli --profile <p>". The
// per-command --as is positional within each embedded command; render it with
// AsFlag where the command places it.
func (c CommandContext) CLIHead() string {
	if c.Profile == "" {
		return "lark-cli"
	}
	return "lark-cli --profile " + c.Profile
}

// AsFlag renders " --as <identity>" (with a leading space) for the resolved
// identity, or "" when none is set — so a hint can splice it into an embedded
// command wherever that command expects the identity flag.
func (c CommandContext) AsFlag() string {
	if c.Identity == "" {
		return ""
	}
	return " --as " + string(c.Identity)
}

// FlagArgs renders the global flags as discrete argv tokens — ["--profile", P,
// "--as", I] — for a structured {Command, Args} recovery form (e.g. status's
// nextActionCommand). Each of profile/identity is omitted when empty.
func (c CommandContext) FlagArgs() []string {
	var out []string
	if c.Profile != "" {
		out = append(out, "--profile", c.Profile)
	}
	if c.Identity != "" {
		out = append(out, "--as", string(c.Identity))
	}
	return out
}
