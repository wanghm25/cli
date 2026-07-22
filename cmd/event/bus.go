// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/credential"
	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/bus"
	"github.com/larksuite/cli/internal/event/transport"
)

// NewCmdBus creates the hidden `event _bus` daemon subcommand, forked by the consume client; fork argv lives in consume/startup.go.
func NewCmdBus(f *cmdutil.Factory) *cobra.Command {
	var domain string

	cmd := &cobra.Command{
		Use:    "_bus",
		Short:  "Internal event bus daemon (do not call directly)",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := f.Config()
			if err != nil {
				return err
			}

			// Sanitize AppID: an unsanitized value could escape events/ via ".." or separators.
			eventsDir := filepath.Join(core.GetConfigDir(), "events", event.SanitizeAppID(cfg.AppID))

			logger, err := bus.SetupBusLogger(eventsDir)
			if err != nil {
				return errs.NewInternalError(errs.SubtypeFileIO,
					"set up bus logger: %s", err).WithCause(err)
			}

			tr := transport.New()
			b := bus.NewBus(cfg.AppID, cfg.AppSecret, domain, tr, logger)

			// Wires the real-time identity gate + BindUser (spec §4.4).
			// f.Credential.ResolveToken resolves a UAT via the SAME
			// credential chain every other command uses (respects a
			// configured extension credential provider, not just the
			// built-in keychain-backed default) and is uncached for UAT —
			// see internal/credential/default_provider.go's resolveUAT doc
			// ("may be refreshed between calls"). userOpenID is accepted
			// for parity with the identity gate's seam (and potential
			// future use/diagnostics) but isn't threaded through:
			// ResolveToken has no per-user-open-id parameter and instead
			// re-resolves the active account itself via the same
			// fresh-config-read path the gate's resolveCurrent just used,
			// so the two can never disagree on which stored token gets
			// fetched within one connect/reconnect action.
			b.SetIdentityProviders(func(ctx context.Context, appID, userOpenID string) (string, error) {
				result, err := f.Credential.ResolveToken(ctx, credential.NewTokenSpec(core.AsUser, appID))
				if err != nil {
					return "", err
				}
				return result.Token, nil
			})

			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
			defer signal.Stop(sigCh)
			go func() {
				select {
				case <-sigCh:
					cancel()
				case <-ctx.Done():
				}
			}()

			if err := b.Run(ctx); err != nil {
				if _, ok := errs.ProblemOf(err); ok {
					return err
				}
				return errs.NewInternalError(errs.SubtypeUnknown,
					"event bus daemon exited: %s", err).WithCause(err)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&domain, "domain", "", "API domain")
	_ = cmd.Flags().MarkHidden("domain")
	cmdutil.SetRisk(cmd, "write")

	return cmd
}
