// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package cmdutil

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"

	extcred "github.com/larksuite/cli/extension/credential"
	"github.com/larksuite/cli/extension/fileio"
	"github.com/larksuite/cli/internal/auth"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/credential"
	"github.com/larksuite/cli/internal/keychain"
	"github.com/larksuite/cli/internal/registry"
	"github.com/larksuite/cli/internal/riskcontrol"
	"github.com/larksuite/cli/internal/runtimeplan"
	_ "github.com/larksuite/cli/internal/security/contentsafety" // register content safety provider
	"github.com/larksuite/cli/internal/transport"
	_ "github.com/larksuite/cli/internal/vfs/localfileio" // register default FileIO provider
)

var (
	initRegistryWithBrand         = registry.InitWithBrand
	initEmbeddedRegistryWithBrand = registry.InitEmbeddedWithBrand
)

// NewDefault creates a production Factory with cached closures.
// Initialization follows a credential-first order:
//
//	Phase 1: HttpClient (no credential dependency)
//	Phase 2: Credential (sole data source for account info)
//	Phase 3: Config derived from Credential
//	Phase 4: LarkClient derived from Credential and workspace policy
func NewDefault(streams *IOStreams, inv InvocationContext) *Factory {
	// Preserve the established standalone Factory behavior. Product-specific
	// runtime selection belongs to the CLI composition root, which calls
	// NewDefaultWithRuntimePlan with one immutable startup snapshot.
	core.SetCurrentWorkspace(core.DetectWorkspaceFromEnv(os.Getenv))
	return newDefaultWithRuntimePlan(streams, inv, nil, runtimeplan.Default(), false)
}

// NewDefaultWithRuntimePlan creates a production Factory from the same
// immutable Profile snapshot and source-neutral plan used by startup routing.
func NewDefaultWithRuntimePlan(
	streams *IOStreams,
	inv InvocationContext,
	profileConfig *core.MultiAppConfig,
	plan *runtimeplan.Plan,
) *Factory {
	return newDefaultWithRuntimePlan(streams, inv, profileConfig, plan, true)
}

func newDefaultWithRuntimePlan(
	streams *IOStreams,
	inv InvocationContext,
	profileConfig *core.MultiAppConfig,
	plan *runtimeplan.Plan,
	useProfileSnapshot bool,
) *Factory {
	streams = normalizeStreams(streams)
	f := &Factory{
		Keychain:    keychain.Default(),
		Invocation:  inv,
		IOStreams:   streams,
		runtimePlan: runtimeplan.Ensure(plan),
	}

	// Inject workspace-aware dir into keychain's log system.
	// This breaks the core↔keychain import cycle by using a function variable.
	keychain.RuntimeDirFunc = core.GetRuntimeDir

	// Phase 0: FileIO provider (no dependency)
	f.FileIOProvider = fileio.GetProvider()
	workspaceConfig := core.NewConfigSnapshot()
	if profileConfig != nil {
		workspaceConfig = core.NewConfigSnapshotFrom(profileConfig)
	}

	// Phase 1: HttpClient (no credential dependency)
	f.HttpClient = cachedHttpClientFunc(f, workspaceConfig)

	// Phase 2: Credential (sole data source)
	// Keychain is read via closure so callers can replace f.Keychain after construction.
	f.Credential = buildCredentialProvider(credentialDeps{
		Keychain:              func() keychain.KeychainAccess { return f.Keychain },
		Profile:               inv.Profile,
		HttpClient:            f.HttpClient,
		ErrOut:                f.IOStreams.ErrOut,
		RuntimePlan:           f.runtimePlan,
		ProfileConfigSnapshot: profileConfig,
		UseProfileSnapshot:    useProfileSnapshot,
	})

	// Phase 3: Runtime config contains resolved account data only.
	f.Config = sync.OnceValues(func() (*core.CliConfig, error) {
		acct, err := f.Credential.ResolveAccount(context.Background())
		if err != nil {
			return nil, err
		}
		cfg := acct.ToCliConfig()
		if f.runtimePlan.AllowsRemoteMetadata() {
			initRegistryWithBrand(cfg.Brand)
		} else {
			// Defense in depth for callers that construct a Factory directly
			// instead of going through cmd/build's composition root.
			initEmbeddedRegistryWithBrand(cfg.Brand)
		}
		return cfg, nil
	})

	// Phase 4: LarkClient composes account data and workspace policy at the SDK
	// transport boundary.
	f.LarkClient = cachedLarkClientFunc(f, workspaceConfig)

	return f
}

// safeRedirectPolicy prevents credential headers from being forwarded
// when a response redirects to a different host (e.g. Lark API 302 → CDN).
// Strips Authorization, X-Lark-MCP-UAT, and X-Lark-MCP-TAT on cross-host
// redirects; other headers like X-Cli-* pass through.
func safeRedirectPolicy(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return fmt.Errorf("too many redirects")
	}
	if len(via) > 0 && req.URL.Host != via[0].URL.Host {
		req.Header.Del("Authorization")
		req.Header.Del("X-Lark-MCP-UAT")
		req.Header.Del("X-Lark-MCP-TAT")
	}
	return nil
}

// warnIfProxied is a test seam for the proxy-warning gate. Production wires it
// to transport.WarnIfProxied; tests swap in a spy to count invocations. It is
// needed because the real function is guarded by an internal sync.Once, so
// calling it directly would only fire on the first test (see
// factory_proxy_warn_test.go). The terminal check is the IOStreams
// .StderrIsTerminal field, which tests set directly.
var warnIfProxied = transport.WarnIfProxied

func cachedHttpClientFunc(f *Factory, workspaceConfig workspaceConfigSource) func() (*http.Client, error) {
	return sync.OnceValues(func() (*http.Client, error) {
		if f.IOStreams.StderrIsTerminal {
			warnIfProxied(f.IOStreams.ErrOut)
		}

		hostSignalSource := resolveSDKHostSignalSource(workspaceConfig)

		var rt http.RoundTripper = transport.Shared()
		var err error
		rt, err = applyRuntimePlan(f, rt)
		if err != nil {
			return nil, err
		}
		// Risk control remains the final trusted header boundary before either
		// the ordinary network transport or the managed proxy data plane.
		rt = riskcontrol.NewTransport(rt, hostSignalSource)
		rt = &RetryTransport{Base: rt}
		rt = &SecurityHeaderTransport{Base: rt}
		rt = &auth.SecurityPolicyTransport{Base: rt} // Add our global response interceptor
		rt = wrapWithExtension(rt)
		client := &http.Client{
			Transport:     rt,
			Timeout:       30 * time.Second,
			CheckRedirect: safeRedirectPolicy,
		}
		return client, nil
	})
}

func cachedLarkClientFunc(f *Factory, workspaceConfig workspaceConfigSource) func() (*lark.Client, error) {
	return sync.OnceValues(func() (*lark.Client, error) {
		acct, err := f.Credential.ResolveAccount(context.Background())
		if err != nil {
			return nil, err
		}
		opts := []lark.ClientOptionFunc{
			lark.WithEnableTokenCache(false),
			lark.WithLogLevel(larkcore.LogLevelError),
			lark.WithHeaders(BaseSecurityHeaders()),
		}
		if f.IOStreams.StderrIsTerminal {
			warnIfProxied(f.IOStreams.ErrOut)
		}
		hostSignalSource := resolveSDKHostSignalSource(workspaceConfig)
		var sdkBase http.RoundTripper = transport.Shared()
		sdkBase, err = applyRuntimePlan(f, sdkBase)
		if err != nil {
			return nil, err
		}
		// The innermost SDK boundary always strips reserved host-signal headers;
		// a nil source makes it strip-only when workspace policy disables signal
		// collection. A managed runtime applies its data-plane policy after this
		// boundary so trusted signals remain associated with the original request.
		sdkBase = riskcontrol.NewTransport(sdkBase, hostSignalSource)
		sdkTransport := wrapSDKTransport(sdkBase)
		opts = append(opts, lark.WithHttpClient(&http.Client{
			Transport:     sdkTransport,
			CheckRedirect: safeRedirectPolicy,
		}))
		ep := core.ResolveEndpoints(acct.Brand)
		opts = append(opts, lark.WithOpenBaseUrl(ep.Open))
		return lark.NewClient(acct.AppID, credential.RuntimeAppSecret(acct.AppSecret), opts...), nil
	})
}

func wrapSDKTransport(next http.RoundTripper) http.RoundTripper {
	var sdkTransport http.RoundTripper = &RetryTransport{Base: next}
	sdkTransport = &UserAgentTransport{Base: sdkTransport}
	sdkTransport = &BuildHeaderTransport{Base: sdkTransport}
	sdkTransport = &auth.SecurityPolicyTransport{Base: sdkTransport}
	sdkTransport = wrapWithExtension(sdkTransport)
	return sdkTransport
}

func applyRuntimePlan(f *Factory, base http.RoundTripper) (http.RoundTripper, error) {
	if f == nil {
		return base, nil
	}
	return runtimeplan.Ensure(f.runtimePlan).Wrap(base)
}

type credentialDeps struct {
	Keychain              func() keychain.KeychainAccess
	Profile               string
	HttpClient            func() (*http.Client, error)
	ErrOut                io.Writer
	RuntimePlan           *runtimeplan.Plan
	ProfileConfigSnapshot *core.MultiAppConfig
	UseProfileSnapshot    bool
}

func buildCredentialProvider(deps credentialDeps) *credential.CredentialProvider {
	plan := runtimeplan.Ensure(deps.RuntimePlan)
	providers := extcred.Providers()
	localAcct := credential.NewDefaultAccountProvider(deps.Keychain, deps.Profile)
	if deps.UseProfileSnapshot {
		localAcct = credential.NewDefaultAccountProviderFromSnapshot(deps.Keychain, deps.Profile, deps.ProfileConfigSnapshot)
	}
	localToken := credential.NewDefaultTokenProvider(localAcct, deps.HttpClient, deps.ErrOut)
	var defaultAcct credential.DefaultAccountResolver = localAcct
	var defaultToken credential.DefaultTokenResolver = localToken

	if startupErr := plan.StartupError(); startupErr != nil {
		providers = []extcred.Provider{&runtimePlanErrorProvider{err: startupErr}}
		defaultAcct = nil
		defaultToken = nil
	} else if provider, replace := plan.CredentialProvider(); provider != nil {
		if replace {
			providers = []extcred.Provider{provider}
			defaultAcct = nil
			defaultToken = nil
		} else {
			providers = append([]extcred.Provider{provider}, providers...)
		}
	}
	// NOTE: Do not pass deps.ErrOut as warnOut. Credential resolution
	// happens before the command runs, so any plain-text warning written
	// to stderr would break the JSON envelope contract that AI agents
	// depend on. enrichUserInfo failures are already non-fatal (the
	// provider clears unverified identity fields), so silencing the
	// warning is safe.
	return credential.NewCredentialProvider(providers, defaultAcct, defaultToken, deps.HttpClient)
}

type runtimePlanErrorProvider struct{ err error }

func (p *runtimePlanErrorProvider) Name() string { return "runtime-policy" }

func (p *runtimePlanErrorProvider) ResolveAccount(context.Context) (*extcred.Account, error) {
	return nil, p.err
}

func (p *runtimePlanErrorProvider) ResolveToken(context.Context, extcred.TokenSpec) (*extcred.Token, error) {
	return nil, p.err
}
