// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	eventlib "github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/schemas"
	"github.com/larksuite/cli/internal/output"
)

// resolveSchemaJSON returns the final JSON Schema for an EventKey (reflected base, V2-wrapped for Native, overlay applied); orphans lists unresolved FieldOverrides pointers.
func resolveSchemaJSON(def *eventlib.KeyDefinition) (json.RawMessage, []string, error) {
	spec, isNative := pickSpec(def.Schema)
	if spec == nil {
		return nil, nil, nil
	}

	base, err := renderSpec(spec)
	if err != nil {
		return nil, nil, err
	}
	if base == nil {
		return nil, nil, nil
	}

	if isNative {
		base = schemas.WrapV2Envelope(base)
	}

	if len(def.Schema.FieldOverrides) > 0 {
		var parsed map[string]interface{}
		if err := json.Unmarshal(base, &parsed); err != nil {
			return nil, nil, errs.NewInternalError(errs.SubtypeUnknown,
				"parse base schema for field overrides: %s", err).WithCause(err)
		}
		orphans := schemas.ApplyFieldOverrides(parsed, def.Schema.FieldOverrides)
		out, err := json.Marshal(parsed)
		if err != nil {
			return nil, nil, errs.NewInternalError(errs.SubtypeUnknown,
				"serialize schema with field overrides: %s", err).WithCause(err)
		}
		return out, orphans, nil
	}

	return base, nil, nil
}

// pickSpec returns the non-nil spec and whether it is Native (requires V2 envelope wrap).
func pickSpec(s eventlib.SchemaDef) (*eventlib.SchemaSpec, bool) {
	if s.Native != nil {
		return s.Native, true
	}
	if s.Custom != nil {
		return s.Custom, false
	}
	return nil, false
}

// renderSpec produces a JSON Schema from Type (reflected) or Raw (copied).
func renderSpec(s *eventlib.SchemaSpec) (json.RawMessage, error) {
	if s.Type != nil {
		return schemas.FromType(s.Type), nil
	}
	if len(s.Raw) > 0 {
		buf := make(json.RawMessage, len(s.Raw))
		copy(buf, s.Raw)
		return buf, nil
	}
	return nil, errs.NewInternalError(errs.SubtypeUnknown, "schemaSpec has neither Type nor Raw")
}

func NewCmdSchema(f *cmdutil.Factory) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "schema <EventKey>",
		Short: "Show details for an EventKey",
		Long:  "Display detailed information about an EventKey including type, events, parameters, and response schema. Use --json for machine-readable output.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSchema(f, args[0], asJSON)
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "Emit the EventKey definition + resolved schema as JSON (for AI / scripts)")
	cmdutil.SetRisk(cmd, "read")
	return cmd
}

func runSchema(f *cmdutil.Factory, key string, asJSON bool) error {
	def, ok := eventlib.Lookup(key)
	if !ok {
		return unknownEventKeyErr(key)
	}

	if asJSON {
		return writeSchemaJSON(f, def)
	}

	out := f.IOStreams.Out

	fmt.Fprintf(out, "Key:         %s\n", def.Key)
	if def.Description != "" {
		fmt.Fprintf(out, "Description: %s\n", def.Description)
	}
	fmt.Fprintf(out, "Event:       %s\n", def.EventType)

	if def.PreConsume != nil {
		fmt.Fprintf(out, "Pre-consume: yes\n")
	}

	if len(def.Scopes) > 0 {
		fmt.Fprintf(out, "\nRequired Scopes:\n")
		for _, s := range def.Scopes {
			fmt.Fprintf(out, "  - %s\n", s)
		}
	}

	if len(def.RequiredConsoleEvents) > 0 {
		fmt.Fprintf(out, "\nRequired Console Events (must be enabled in developer console):\n")
		for _, e := range def.RequiredConsoleEvents {
			fmt.Fprintf(out, "  - %s\n", e)
		}
	}

	renderRefinedSubscriptionText(out, def)

	if len(def.Params) > 0 {
		fmt.Fprintf(out, "\nParameters:\n")
		w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		fmt.Fprintf(w, "  NAME\tTYPE\tREQUIRED\tSUB-KEY\tDEFAULT\tDESCRIPTION\n")
		for _, p := range def.Params {
			required := "no"
			if p.Required {
				required = "yes"
			}
			subKey := "no"
			if p.SubscriptionKey {
				subKey = "yes"
			}
			defaultVal := p.Default
			if defaultVal == "" {
				defaultVal = "-"
			}
			desc := p.Description
			if desc == "" {
				desc = "-"
			}
			fmt.Fprintf(w, "  %s\t%s\t%s\t%s\t%s\t%s\n", p.Name, p.Type, required, subKey, defaultVal, desc)
		}
		w.Flush()

		// Inline Values below the table so AI consumers see allowed enum/multi values without --json.
		for _, p := range def.Params {
			if len(p.Values) == 0 {
				continue
			}
			fmt.Fprintf(out, "\n  %s values:\n", p.Name)
			vw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
			for _, v := range p.Values {
				fmt.Fprintf(vw, "    %s\t%s\n", v.Value, v.Desc)
			}
			vw.Flush()
		}
	}

	resolved, _, err := resolveSchemaJSON(def)
	if err != nil {
		return err
	}
	if resolved != nil {
		fmt.Fprintf(out, "\nOutput Schema:\n")
		printIndentedJSON(out, resolved)
	} else {
		fmt.Fprintf(out, "\nOutput Schema: (schema not declared)\n")
		if def.Schema.Native != nil {
			fmt.Fprintf(out, "  Consumers receive the V2 envelope: {schema, header, event}.\n")
			fmt.Fprintf(out, "  Inspect real payloads via `lark-cli event consume %s`.\n", def.Key)
		}
	}

	return nil
}

// printIndentedJSON pretty-prints raw JSON with a 2-space leading indent.
func printIndentedJSON(out io.Writer, raw json.RawMessage) {
	var parsed json.RawMessage
	if err := json.Unmarshal(raw, &parsed); err != nil {
		fmt.Fprintln(out, "  <invalid JSON>")
		return
	}
	formatted, err := json.MarshalIndent(parsed, "  ", "  ")
	if err != nil {
		return
	}
	fmt.Fprintf(out, "  %s\n", string(formatted))
}

// ConditionalScope documents a scope that is only required under specific
// flag/identity combinations, as opposed to KeyDefinition.Scopes
// which are always required. Currently static: every refined key discloses
// the same encrypt-key read requirement for --include-resource-data + --as user.
type ConditionalScope struct {
	Scope string `json:"scope"`
	When  string `json:"when"`
}

// RiskDisclosure discloses the per-key risk semantics that a static command
// annotation cannot express by itself: ordinary EventKeys are read-only, while a
// refined EventKey has write-level startup side effects. NewCmdConsume registers
// the conservative static command risk as "write"; runConsume narrows it to
// "read" for an ordinary key once the EventKey is resolved.
type RiskDisclosure struct {
	OrdinaryEventKey  string `json:"ordinary_event_key"`
	Effective         string `json:"effective"`
	Reason            string `json:"reason"`
	DryRunRecommended bool   `json:"dry_run_recommended"`
}

// PayloadOptions documents the resource-data payload flags that apply to
// refined-key subscription management (`event subscription create/update`).
type PayloadOptions struct {
	IncludeResourceDataDefault bool   `json:"include_resource_data_default"`
	IncludeResourceDataFlag    string `json:"include_resource_data_flag"`
}

// DryRunInfo documents the dry-run contract for refined-key subscription management.
type DryRunInfo struct {
	Supported bool   `json:"supported"`
	Example   string `json:"example"`
}

// SubscriptionInfo documents refined-key subscription-management affordances.
type SubscriptionInfo struct {
	PayloadOptions PayloadOptions `json:"payload_options"`
	DryRun         DryRunInfo     `json:"dry_run"`
}

// refinedDryRunExample returns the copy-pasteable dry-run command for a refined
// key, anchored on its first key template's example when present so it is a
// concrete command rather than a placeholder.
func refinedDryRunExample(def *eventlib.KeyDefinition) string {
	exampleKey := def.Key
	if len(def.KeyTemplates) > 0 {
		exampleKey = def.KeyTemplates[0].Example
	}
	return fmt.Sprintf("lark-cli event subscription create %s --dry-run --json", exampleKey)
}

// refinedConditionalScopes/refinedRiskDisclosure/refinedSubscriptionInfo are the
// single source of truth for a refined key's subscription-management
// disclosures, so the --json and human-readable renderings never drift apart.
func refinedConditionalScopes() []ConditionalScope {
	return []ConditionalScope{
		{Scope: "event:encrypt_key:read", When: "--include-resource-data set with --as user"},
	}
}

func refinedRiskDisclosure() *RiskDisclosure {
	return &RiskDisclosure{
		OrdinaryEventKey:  "read",
		Effective:         "write",
		Reason:            "refined consume may create, reuse, reactivate, or bind remote resources",
		DryRunRecommended: true,
	}
}

func refinedSubscriptionInfo(dryRunExample string) *SubscriptionInfo {
	return &SubscriptionInfo{
		PayloadOptions: PayloadOptions{
			IncludeResourceDataDefault: false,
			IncludeResourceDataFlag:    "--include-resource-data",
		},
		DryRun: DryRunInfo{Supported: true, Example: dryRunExample},
	}
}

// renderRefinedSubscriptionText surfaces, for a refined-subscription key, the
// same disclosures the --json output carries (resource type, conditional
// scopes, effective write risk, payload options, dry-run, next action), one
// concise line per field. It prints nothing for a legacy key, so non-refined
// text output is unchanged.
func renderRefinedSubscriptionText(out io.Writer, def *eventlib.KeyDefinition) {
	if !def.RefinedSubscription {
		return
	}

	fmt.Fprintf(out, "\nRefined Subscription: yes\n")
	if def.ResourceType != "" {
		fmt.Fprintf(out, "Resource Type: %s\n", def.ResourceType)
	}

	fmt.Fprintf(out, "Conditional Scopes:\n")
	for _, cs := range refinedConditionalScopes() {
		fmt.Fprintf(out, "  - %s (when %s)\n", cs.Scope, cs.When)
	}

	risk := refinedRiskDisclosure()
	dryRunNote := ""
	if risk.DryRunRecommended {
		dryRunNote = " (dry-run recommended)"
	}
	fmt.Fprintf(out, "Risk: ordinary %s, effective %s — %s%s\n",
		risk.OrdinaryEventKey, risk.Effective, risk.Reason, dryRunNote)

	dryRunExample := refinedDryRunExample(def)
	sub := refinedSubscriptionInfo(dryRunExample)
	fmt.Fprintf(out, "Payload Options: %s (include_resource_data default: %t)\n",
		sub.PayloadOptions.IncludeResourceDataFlag, sub.PayloadOptions.IncludeResourceDataDefault)
	fmt.Fprintf(out, "Dry Run: supported — %s\n", sub.DryRun.Example)
	fmt.Fprintf(out, "Next Action: run `%s` before consume\n", dryRunExample)
}

// writeSchemaJSON emits the EventKey definition plus resolved schema; jq_root_path tells callers whether fields live at `.` or `.event`.
//
// payload embeds *eventlib.KeyDefinition, so refined_subscription/resource_type/
// key_templates[]/auth_types/scopes are already promoted into the
// JSON output unchanged for every key — no new code needed for those. Only
// ConditionalScopes/Risk/Subscription/NextAction are genuinely new here, and
// they use pointer/slice/string types so `omitempty` actually omits them
// (Go's encoding/json omitempty has no effect on plain struct fields) — all
// four are left unset for non-refined keys, so legacy key JSON output is
// byte-for-byte unchanged.
func writeSchemaJSON(f *cmdutil.Factory, def *eventlib.KeyDefinition) error {
	type payload struct {
		*eventlib.KeyDefinition
		ResolvedSchema    json.RawMessage    `json:"resolved_output_schema,omitempty"`
		JQRootPath        string             `json:"jq_root_path,omitempty"`
		ConditionalScopes []ConditionalScope `json:"conditional_scopes,omitempty"`
		Risk              *RiskDisclosure    `json:"risk,omitempty"`
		Subscription      *SubscriptionInfo  `json:"subscription,omitempty"`
		NextAction        string             `json:"next_action,omitempty"`
	}
	resolved, _, err := resolveSchemaJSON(def)
	if err != nil {
		return err
	}
	var jqRootPath string
	if resolved != nil {
		// Native → V2 envelope ⇒ `.event.xxx`; Custom → flat ⇒ `.`.
		_, isNative := pickSpec(def.Schema)
		jqRootPath = "."
		if isNative {
			jqRootPath = ".event"
		}
	}

	p := payload{
		KeyDefinition:  def,
		ResolvedSchema: resolved,
		JQRootPath:     jqRootPath,
	}

	if def.RefinedSubscription {
		dryRunExample := refinedDryRunExample(def)
		p.ConditionalScopes = refinedConditionalScopes()
		p.Risk = refinedRiskDisclosure()
		p.Subscription = refinedSubscriptionInfo(dryRunExample)
		p.NextAction = fmt.Sprintf("run `%s` before consume", dryRunExample)
	}

	output.PrintJson(f.IOStreams.Out, p)
	return nil
}
