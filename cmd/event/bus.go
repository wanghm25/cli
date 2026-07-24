// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"context"
	"errors"
	"fmt"
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

// errUATUnverifiable is verifyUATBelongsToUser's fail-closed rejection when a
// resolved UAT's owner cannot be positively proven (no way to resolve its
// open_id, or the resolution itself failed). "Cannot prove ownership" fails
// closed, not open. Names no token value.
var errUATUnverifiable = errors.New("event bus: could not prove the resolved user access token belongs to the requested user; refusing to use an unverifiable token")

// verifyUATBelongsToUser confirms a freshly resolved UAT actually belongs to
// userOpenID. ResolveToken re-resolves the active account by appID alone, so a
// profile / active-user switch between the identity gate's resolveCurrent and
// this mint could otherwise return a DIFFERENT user's token. Verification is
// positive and fail-closed:
//   - The keychain stores each user's UAT under (appID, userOpenID). When a
//     token IS stored for the requested user, a resolved token that differs
//     provably belongs to someone else and is rejected; an exact match is
//     accepted (the fast path).
//   - With NO stored copy to compare against (e.g. an extension credential
//     provider that keeps nothing in the keychain), the token is not trusted
//     blindly: proveOpenID resolves the token's own open_id (any valid UAT can
//     call user_info) and it must equal userOpenID. A mismatch, a resolution
//     failure, or no way to resolve it at all is rejected — ownership that
//     cannot be proven fails closed.
//
// Never compares or logs the token value beyond an equality check against the
// same user's own stored copy, and never logs the resolved open_id.
func verifyUATBelongsToUser(ctx context.Context, appID, userOpenID, token string, proveOpenID func(context.Context, string) (string, error)) error {
	if userOpenID == "" || token == "" {
		return nil
	}
	if stored := getStoredUAToken(appID, userOpenID); stored != nil {
		if stored.AccessToken != token {
			return errUATUserMismatch
		}
		return nil
	}
	// No stored copy: positively prove the token's owner instead of trusting it.
	if proveOpenID == nil {
		return errUATUnverifiable
	}
	gotOpenID, err := proveOpenID(ctx, token)
	if err != nil {
		return fmt.Errorf("%w: %v", errUATUnverifiable, err)
	}
	if gotOpenID != userOpenID {
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
			// a DIFFERENT user's token. verifyUATBelongsToUser closes that race:
			// the keychain stores each user's UAT under (appID, userOpenID), and
			// when nothing is stored it positively resolves the token's own
			// open_id (via user_info) and fails closed unless it matches — so no
			// wrong-user or unprovable UAT ever reaches
			// Renew/Reactivate/Get/BindUser or the Hello-time encrypt_key fetch.
			proveOpenID := func(ctx context.Context, token string) (string, error) {
				return f.Credential.VerifyUATOpenID(ctx, cfg.Brand, token)
			}
			b.SetIdentityProviders(func(ctx context.Context, appID, userOpenID string) (string, error) {
				result, err := f.Credential.ResolveToken(ctx, credential.NewTokenSpec(core.AsUser, appID))
				if err != nil {
					return "", err
				}
				if err := verifyUATBelongsToUser(ctx, appID, userOpenID, result.Token, proveOpenID); err != nil {
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
