// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/auth"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/credential"
	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/bus"
	"github.com/larksuite/cli/internal/event/transport"
)

// getStoredUAToken is indirected so verifyUATBelongsToUser is testable without
// the OS keychain. Production reads the keychain-backed UAT store, which is
// keyed by (appID, userOpenID). Tests override it.
var getStoredUAToken = auth.GetStoredToken

// errUATUserMismatch is verifyUATBelongsToUser's rejection: the freshly
// resolved UAT provably belongs to a different user than the one the identity
// gate resolved as current. Deliberately names no token value.
var errUATUserMismatch = errors.New("event bus: resolved user access token does not belong to the requested user (the active profile/user may have switched); refusing to use another user's token")

// verifyUATBelongsToUser confirms a freshly resolved UAT actually belongs to
// userOpenID. ResolveToken re-resolves the active account by appID alone, so a
// profile / active-user switch between the identity gate's resolveCurrent and
// this mint could otherwise return a DIFFERENT user's token (review
// #5/#6/#10). The keychain stores each user's UAT under (appID, userOpenID):
// when a token IS stored for the requested user and the resolved token differs,
// it provably belongs to someone else and is rejected. A missing stored token
// (e.g. an extension credential provider that keeps nothing in the keychain,
// which owns its own identity binding) cannot be disproven here and falls back
// to the fresh owner==current gate the caller already applied — never a
// false reject. Never compares or logs the token value beyond an equality
// check against the same user's own stored copy.
func verifyUATBelongsToUser(appID, userOpenID, token string) error {
	if userOpenID == "" || token == "" {
		return nil
	}
	stored := getStoredUAToken(appID, userOpenID)
	if stored != nil && stored.AccessToken != token {
		return errUATUserMismatch
	}
	return nil
}

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

			// Wires the real-time identity gate + BindUser.
			// f.Credential.ResolveToken resolves a UAT via the SAME
			// credential chain every other command uses (respects a
			// configured extension credential provider, not just the
			// built-in keychain-backed default) and is uncached for UAT —
			// see internal/credential/default_provider.go's resolveUAT doc
			// ("may be refreshed between calls").
			//
			// ResolveToken has no per-user-open-id parameter: it re-resolves
			// the active account itself by appID. The identity gate resolves
			// current=(appID,userOpenID) FIRST and passes userOpenID here, so
			// a profile / active-user switch between those two steps could mint
			// a DIFFERENT user's token. verifyUATBelongsToUser closes that race
			// (review #5/#6/#10): the keychain stores each user's UAT under
			// (appID, userOpenID), so a resolved token that provably belongs to
			// another user is rejected — no wrong-user UAT ever reaches
			// Renew/Reactivate/Get/BindUser or the Hello-time encrypt_key fetch.
			b.SetIdentityProviders(func(ctx context.Context, appID, userOpenID string) (string, error) {
				result, err := f.Credential.ResolveToken(ctx, credential.NewTokenSpec(core.AsUser, appID))
				if err != nil {
					return "", err
				}
				if err := verifyUATBelongsToUser(appID, userOpenID, result.Token); err != nil {
					return "", err
				}
				return result.Token, nil
			})

			// Wires the *lark.Client the lifecycle action needs
			// for its single Reactivate/Renew/Get call per event.
			// f.LarkClient() is the SAME primitive every other
			// `event subscription`/`event consume` command builds its
			// SubscriptionClient from --
			// bound to this SAME cfg.AppID/AppSecret pair the bus itself
			// was just constructed with (a bus is per-app). A failure here
			// degrades to summary-only lifecycle handling (no Reactivate/
			// Renew/Get ever attempted) rather than failing bus startup --
			// the bus's core job (WS connect + local fan-out) must not
			// depend on this optional remote-management capability.
			if sdk, sdkErr := f.LarkClient(); sdkErr == nil {
				b.SetSubscriptionClient(sdk)
			} else {
				logger.Printf("WARN: could not build a SubscriptionClient for lifecycle management (%v); lifecycle events will be summary-only", sdkErr)
			}

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
