// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	eventlib "github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/schemas"

	_ "github.com/larksuite/cli/events"
)

type approvalSchemaJSONPayload struct {
	JQRootPath           string                           `json:"jq_root_path"`
	AuthTypes            []string                         `json:"auth_types"`
	Scopes               []string                         `json:"scopes"`
	Params               []approvalSchemaJSONParam        `json:"params"`
	ResolvedOutputSchema approvalSchemaJSONResolvedSchema `json:"resolved_output_schema"`
}

type approvalSchemaJSONParam struct {
	Name            string `json:"name"`
	Type            string `json:"type"`
	Required        bool   `json:"required"`
	SubscriptionKey bool   `json:"subscription_key"`
}

type approvalSchemaJSONResolvedSchema struct {
	Properties map[string]approvalSchemaJSONProperty `json:"properties"`
}

type approvalSchemaJSONProperty struct {
	Format string `json:"format"`
}

func TestRunSchema_ProcessedKey_Text(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})

	if err := runSchema(f, "im.message.receive_v1", false); err != nil {
		t.Fatalf("runSchema: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"Key:", "im.message.receive_v1",
		"Event:", "im.message.receive_v1",
		"Output Schema:",
		`"message_id"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("schema output missing %q; got:\n%s", want, out)
		}
	}
}

func TestRunSchema_NativeKey_WrapsEnvelope(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})

	if err := runSchema(f, "im.message.message_read_v1", false); err != nil {
		t.Fatalf("runSchema: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"Output Schema:",
		`"schema"`,
		`"header"`,
		`"event"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("native schema output missing %q; got:\n%s", want, out)
		}
	}
}

func TestRunSchema_UnknownKey_SuggestsAlternatives(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})

	err := runSchema(f, "im.message.recieve_v1", false)
	if err == nil {
		t.Fatal("expected error for unknown key")
	}
	msg := err.Error()
	if !strings.Contains(msg, "unknown EventKey") {
		t.Errorf("error should mention unknown EventKey: %q", msg)
	}
	if !strings.Contains(msg, "im.message.receive_v1") {
		t.Errorf("error should suggest the real key name (typo correction): %q", msg)
	}
}

func TestRunSchema_JSONOutput(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})

	if err := runSchema(f, "im.message.receive_v1", true); err != nil {
		t.Fatalf("runSchema json: %v", err)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, stdout.String())
	}
	for _, field := range []string{"key", "event_type", "schema", "resolved_output_schema"} {
		if _, ok := payload[field]; !ok {
			t.Errorf("JSON output missing field %q: %+v", field, payload)
		}
	}
	if payload["key"] != "im.message.receive_v1" {
		t.Errorf("key = %v, want im.message.receive_v1", payload["key"])
	}
}

func TestRunSchema_ReceiveMessageAgentFieldsJSON(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})

	if err := runSchema(f, "im.message.receive_v1", true); err != nil {
		t.Fatalf("runSchema json: %v", err)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, stdout.String())
	}
	resolved := payload["resolved_output_schema"].(map[string]interface{})
	props := resolved["properties"].(map[string]interface{})
	for _, field := range []string{
		"root_id",
		"thread_id",
		"reply_to",
		"sender_type",
		"mentions",
	} {
		if _, ok := props[field]; !ok {
			t.Errorf("receive schema missing field %q", field)
		}
	}
	msgDesc := props["message_id"].(map[string]interface{})["description"].(string)
	if !strings.Contains(msgDesc, "Recommended idempotency key") {
		t.Errorf("message_id description should guide deduplication, got %q", msgDesc)
	}
	eventDesc := props["event_id"].(map[string]interface{})["description"].(string)
	if strings.Contains(eventDesc, "safe for deduplication") {
		t.Errorf("event_id description should not recommend deduplication, got %q", eventDesc)
	}
}

func TestRunSchema_TaskUpdateUserAccessJSON(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})

	if err := runSchema(f, "task.task.update_user_access_v2", true); err != nil {
		t.Fatalf("runSchema json: %v", err)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, stdout.String())
	}
	if payload["jq_root_path"] != ".event" {
		t.Errorf("jq_root_path = %v, want .event", payload["jq_root_path"])
	}
	if payload["single_consumer"] != true {
		t.Errorf("single_consumer = %v, want true", payload["single_consumer"])
	}
	resolved := payload["resolved_output_schema"].(map[string]interface{})
	props := resolved["properties"].(map[string]interface{})
	eventProps := props["event"].(map[string]interface{})["properties"].(map[string]interface{})
	if got := eventProps["task_guid"].(map[string]interface{})["format"]; got != "task_guid" {
		t.Errorf("task_guid format = %v, want task_guid", got)
	}
	if _, ok := eventProps["event_types"].(map[string]interface{})["items"].(map[string]interface{})["enum"]; !ok {
		t.Fatalf("event_types enum missing in schema: %#v", eventProps["event_types"])
	}
}

func TestRunSchema_ApprovalStatusChangedJSON(t *testing.T) {
	tests := []struct {
		key   string
		scope string
	}{
		{"approval.instance.status_changed_v4", "approval:instance:read"},
		{"approval.task.status_changed_v4", "approval:task:read"},
	}

	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			t.Setenv("LARKSUITE_CLI_CONFIG_DIR", t.TempDir())
			f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})

			if err := runSchema(f, tc.key, true); err != nil {
				t.Fatalf("runSchema json: %v", err)
			}

			var payload approvalSchemaJSONPayload
			if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
				t.Fatalf("output is not valid JSON: %v\n%s", err, stdout.String())
			}
			if payload.JQRootPath != "." {
				t.Errorf("jq_root_path = %v, want .", payload.JQRootPath)
			}
			if got := payload.AuthTypes; !reflect.DeepEqual(got, []string{"user"}) {
				t.Errorf("auth_types = %#v, want user", got)
			}
			if got := payload.Scopes; !reflect.DeepEqual(got, []string{tc.scope}) {
				t.Errorf("scopes = %#v, want %s", got, tc.scope)
			}
			if len(payload.Params) != 1 {
				t.Fatalf("params = %#v, want one subscription_type param", payload.Params)
			}
			param := payload.Params[0]
			if param.Name != "subscription_type" || param.Type != "multi" || param.Required || param.SubscriptionKey {
				t.Fatalf("subscription_type param = %#v, want optional multi non-subscription-key param", param)
			}
			props := payload.ResolvedOutputSchema.Properties
			for _, field := range []string{"type", "event_id", "timestamp", "approval_code", "instance_code", "status", "operate_time"} {
				if _, ok := props[field]; !ok {
					t.Errorf("approval schema missing flat field %q: %+v", field, props)
				}
			}
			if _, ok := props["event"]; ok {
				t.Errorf("approval Custom schema should be flat, got envelope field event: %+v", props)
			}
			if got := props["operate_time"].Format; got != "timestamp_ms" {
				t.Errorf("operate_time format = %v, want timestamp_ms", got)
			}
		})
	}
}

func TestRunSchema_JSONOutput_VCMeetingLifecycleKeys(t *testing.T) {
	for _, key := range []string{
		"vc.meeting.participant_meeting_started_v1",
		"vc.meeting.participant_meeting_joined_v1",
	} {
		t.Run(key, func(t *testing.T) {
			f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})

			if err := runSchema(f, key, true); err != nil {
				t.Fatalf("runSchema json: %v", err)
			}

			var payload map[string]interface{}
			if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
				t.Fatalf("output is not valid JSON: %v\n%s", err, stdout.String())
			}
			if payload["key"] != key {
				t.Errorf("key = %v, want %s", payload["key"], key)
			}
			resolved, ok := payload["resolved_output_schema"].(map[string]interface{})
			if !ok {
				t.Fatalf("resolved_output_schema missing or wrong type: %+v", payload)
			}
			properties, ok := resolved["properties"].(map[string]interface{})
			if !ok {
				t.Fatalf("resolved_output_schema.properties missing or wrong type: %+v", resolved)
			}
			for _, field := range []string{"type", "event_id", "timestamp", "meeting_id", "topic", "meeting_no", "start_time", "calendar_event_id"} {
				if _, ok := properties[field]; !ok {
					t.Errorf("resolved output schema missing field %q: %+v", field, properties)
				}
			}
			if _, ok := properties["end_time"]; ok {
				t.Errorf("resolved output schema should not include end_time for %s: %+v", key, properties)
			}
		})
	}
}

func TestSchema_RendersSubscriptionKeyMarker(t *testing.T) {
	const syntheticKey = "test.evt_sub"
	t.Cleanup(func() { eventlib.UnregisterKeyForTest(syntheticKey) })

	eventlib.RegisterKey(eventlib.KeyDefinition{
		Key:       syntheticKey,
		EventType: syntheticKey,
		Params: []eventlib.ParamDef{
			{Name: "mailbox", SubscriptionKey: true, Description: "subscription id source"},
			{Name: "folders", Description: "filter only"},
		},
		Schema: eventlib.SchemaDef{Native: &eventlib.SchemaSpec{Type: reflect.TypeOf(struct{ X string }{})}},
	})

	f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})
	if err := runSchema(f, syntheticKey, false); err != nil {
		t.Fatalf("runSchema: %v", err)
	}

	out := stdout.String()
	if !strings.Contains(out, "SUB-KEY") {
		t.Errorf("missing SUB-KEY column header in:\n%s", out)
	}

	// Find the mailbox row and verify "yes" is present
	var mailboxRow string
	for _, ln := range strings.Split(out, "\n") {
		if strings.Contains(ln, "mailbox") && !strings.Contains(ln, "NAME") {
			mailboxRow = ln
			break
		}
	}
	if !strings.Contains(mailboxRow, "yes") {
		t.Errorf("mailbox row missing yes SUB-KEY marker: %q", mailboxRow)
	}

	// Find the folders row and verify "no" is present
	var foldersRow string
	for _, ln := range strings.Split(out, "\n") {
		if strings.Contains(ln, "folders") && !strings.Contains(ln, "NAME") {
			foldersRow = ln
			break
		}
	}
	if !strings.Contains(foldersRow, "no") {
		t.Errorf("folders row missing no SUB-KEY marker: %q", foldersRow)
	}
}

func TestSchema_JSON_IncludesSubscriptionKey(t *testing.T) {
	const syntheticKey = "test.evt_json"
	t.Cleanup(func() { eventlib.UnregisterKeyForTest(syntheticKey) })

	eventlib.RegisterKey(eventlib.KeyDefinition{
		Key:       syntheticKey,
		EventType: syntheticKey,
		Params:    []eventlib.ParamDef{{Name: "mailbox", SubscriptionKey: true}},
		Schema:    eventlib.SchemaDef{Native: &eventlib.SchemaSpec{Type: reflect.TypeOf(struct{ X string }{})}},
	})

	f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})
	if err := runSchema(f, syntheticKey, true); err != nil {
		t.Fatalf("runSchema json: %v", err)
	}

	if !strings.Contains(stdout.String(), `"subscription_key"`) {
		t.Errorf("JSON output missing subscription_key field: %s", stdout.String())
	}
	if !strings.Contains(stdout.String(), `true`) {
		t.Errorf("JSON output missing subscription_key: true value: %s", stdout.String())
	}
}

func TestResolveSchemaJSON_CustomWithOverlay(t *testing.T) {
	const syntheticKey = "t.custom.overlay"
	t.Cleanup(func() { eventlib.UnregisterKeyForTest(syntheticKey) })

	type out struct {
		SenderID string `json:"sender_id"`
	}
	eventlib.RegisterKey(eventlib.KeyDefinition{
		Key:       syntheticKey,
		EventType: syntheticKey,
		Schema: eventlib.SchemaDef{
			Custom: &eventlib.SchemaSpec{Type: reflect.TypeOf(out{})},
			FieldOverrides: map[string]schemas.FieldMeta{
				"/sender_id": {Kind: "open_id"},
			},
		},
		Process: func(context.Context, eventlib.APIClient, *eventlib.RawEvent, map[string]string) (json.RawMessage, error) {
			return nil, nil
		},
	})
	def, _ := eventlib.Lookup(syntheticKey)
	resolved, orphans, err := resolveSchemaJSON(def)
	if err != nil || len(orphans) != 0 {
		t.Fatalf("resolve: err=%v orphans=%v", err, orphans)
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal(resolved, &parsed); err != nil {
		t.Fatal(err)
	}
	got := parsed["properties"].(map[string]interface{})["sender_id"].(map[string]interface{})["format"]
	if got != "open_id" {
		t.Errorf("overlay format = %v, want open_id", got)
	}
}

// TestSchemaJSON_RefinedSubscriptionAdditiveFields pins that for a
// RefinedSubscription key, `event schema --json` additively emits
// refined_subscription/key_templates[]/auth_types/scopes/conditional_scopes/
// risk/subscription/next_action on top of the existing resolved_output_schema
// + jq_root_path. refined_subscription/key_templates/auth_types/scopes flow
// for free via the *eventlib.KeyDefinition embed (already asserted here as a
// contract, not because schema.go needs new code for them); conditional_scopes/
// risk/subscription/next_action are genuinely new fields added by this task.
func TestSchemaJSON_RefinedSubscriptionAdditiveFields(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})

	if err := runSchema(f, "im.message.created_v1", true); err != nil {
		t.Fatalf("runSchema json: %v", err)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, stdout.String())
	}

	if payload["refined_subscription"] != true {
		t.Errorf("refined_subscription = %v, want true", payload["refined_subscription"])
	}
	if payload["resource_type"] != "im.message" {
		t.Errorf("resource_type = %v, want im.message", payload["resource_type"])
	}
	if payload["resolved_output_schema"] == nil {
		t.Error("resolved_output_schema must be preserved for refined keys")
	}
	if payload["jq_root_path"] != "." {
		t.Errorf("jq_root_path = %v, want . (Custom schema)", payload["jq_root_path"])
	}

	gotScopes, ok := payload["scopes"].([]interface{})
	wantScopes := []string{"event:subscription:read", "event:subscription:write"}
	if !ok || len(gotScopes) != len(wantScopes) {
		t.Fatalf("scopes = %v, want %v", payload["scopes"], wantScopes)
	}
	for i, want := range wantScopes {
		if gotScopes[i] != want {
			t.Errorf("scopes[%d] = %v, want %q", i, gotScopes[i], want)
		}
	}

	gotAuth, ok := payload["auth_types"].([]interface{})
	if !ok || len(gotAuth) != 2 || gotAuth[0] != "user" || gotAuth[1] != "bot" {
		t.Errorf("auth_types = %v, want [user bot]", payload["auth_types"])
	}

	templatesRaw, ok := payload["key_templates"].([]interface{})
	if !ok || len(templatesRaw) != 2 {
		t.Fatalf("key_templates = %v, want 2 entries", payload["key_templates"])
	}
	firstTemplate, ok := templatesRaw[0].(map[string]interface{})
	if !ok || firstTemplate["template"] != "im.message.created_v1/chat-id/{chat_id}" {
		t.Fatalf("key_templates[0] = %v, want chat-id template first", templatesRaw[0])
	}

	condRaw, ok := payload["conditional_scopes"].([]interface{})
	if !ok || len(condRaw) != 1 {
		t.Fatalf("conditional_scopes = %v, want 1 entry", payload["conditional_scopes"])
	}
	cond, ok := condRaw[0].(map[string]interface{})
	if !ok {
		t.Fatalf("conditional_scopes[0] wrong type: %T", condRaw[0])
	}
	if cond["scope"] != "event:encrypt_key:read" {
		t.Errorf("conditional_scopes[0].scope = %v, want event:encrypt_key:read", cond["scope"])
	}
	wantWhen := "--include-resource-data set with --as user"
	if cond["when"] != wantWhen {
		t.Errorf("conditional_scopes[0].when = %v, want %q", cond["when"], wantWhen)
	}

	risk, ok := payload["risk"].(map[string]interface{})
	if !ok {
		t.Fatalf("risk missing or wrong type: %v", payload["risk"])
	}
	if risk["ordinary_event_key"] != "read" {
		t.Errorf("risk.ordinary_event_key = %v, want read", risk["ordinary_event_key"])
	}
	if risk["effective"] != "write" {
		t.Errorf("risk.effective = %v, want write", risk["effective"])
	}
	wantReason := "refined consume may create, reuse, reactivate, or bind remote resources"
	if risk["reason"] != wantReason {
		t.Errorf("risk.reason = %v, want %q", risk["reason"], wantReason)
	}
	if risk["dry_run_recommended"] != true {
		t.Errorf("risk.dry_run_recommended = %v, want true", risk["dry_run_recommended"])
	}

	sub, ok := payload["subscription"].(map[string]interface{})
	if !ok {
		t.Fatalf("subscription missing or wrong type: %v", payload["subscription"])
	}
	payloadOpts, ok := sub["payload_options"].(map[string]interface{})
	if !ok {
		t.Fatalf("subscription.payload_options missing: %v", sub)
	}
	if payloadOpts["include_resource_data_default"] != false {
		t.Errorf("include_resource_data_default = %v, want false", payloadOpts["include_resource_data_default"])
	}
	if payloadOpts["include_resource_data_flag"] != "--include-resource-data" {
		t.Errorf("include_resource_data_flag = %v, want --include-resource-data", payloadOpts["include_resource_data_flag"])
	}
	dryRun, ok := sub["dry_run"].(map[string]interface{})
	if !ok {
		t.Fatalf("subscription.dry_run missing: %v", sub)
	}
	if dryRun["supported"] != true {
		t.Errorf("subscription.dry_run.supported = %v, want true", dryRun["supported"])
	}
	wantExample := "lark-cli event subscription create im.message.created_v1/chat-id/oc_9f3b1c2d8a --dry-run --json"
	if dryRun["example"] != wantExample {
		t.Errorf("subscription.dry_run.example = %v, want %q", dryRun["example"], wantExample)
	}

	wantNextAction := "run `" + wantExample + "` before consume"
	if payload["next_action"] != wantNextAction {
		t.Errorf("next_action = %v, want %q", payload["next_action"], wantNextAction)
	}
}

// TestSchemaJSON_LegacyKeyUnchanged is the regression half: a
// non-refined (legacy) EventKey must not gain any of the new refined-only
// fields in `event schema --json` output, and its existing fields
// (resolved_output_schema, jq_root_path, scopes, auth_types) must be
// byte-for-byte preserved.
func TestSchemaJSON_LegacyKeyUnchanged(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})

	if err := runSchema(f, "im.message.receive_v1", true); err != nil {
		t.Fatalf("runSchema json: %v", err)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, stdout.String())
	}

	for _, field := range []string{
		"refined_subscription", "key_templates", "resource_type",
		"conditional_scopes", "risk", "subscription", "next_action",
	} {
		if v, present := payload[field]; present {
			t.Errorf("legacy key im.message.receive_v1 must not have %q field; got %v", field, v)
		}
	}
	for _, field := range []string{"key", "event_type", "schema", "resolved_output_schema", "jq_root_path", "scopes", "auth_types"} {
		if _, present := payload[field]; !present {
			t.Errorf("legacy key im.message.receive_v1 unexpectedly lost %q field", field)
		}
	}
	if payload["jq_root_path"] != "." {
		t.Errorf("jq_root_path = %v, want . (Custom schema)", payload["jq_root_path"])
	}
	if payload["key"] != "im.message.receive_v1" {
		t.Errorf("key = %v, want im.message.receive_v1", payload["key"])
	}
}

// TestRunSchema_RefinedSubscription_Text pins the text (non-JSON) parity half:
// a refined key surfaces the same disclosures the --json output carries
// (resource type, conditional scopes, effective write risk, payload options,
// dry-run, next action) in the human-readable output too.
func TestRunSchema_RefinedSubscription_Text(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})

	if err := runSchema(f, "im.message.created_v1", false); err != nil {
		t.Fatalf("runSchema: %v", err)
	}

	out := stdout.String()
	dryRun := "lark-cli event subscription create im.message.created_v1/chat-id/oc_9f3b1c2d8a --dry-run --json"
	for _, want := range []string{
		"Refined Subscription: yes",
		"Resource Type: im.message",
		"event:encrypt_key:read (when --include-resource-data set with --as user)",
		"effective write",
		"refined consume may create, reuse, reactivate, or bind remote resources",
		"Payload Options: --include-resource-data",
		"Dry Run: supported — " + dryRun,
		"Next Action: run `" + dryRun + "` before consume",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("refined schema text missing %q; full output:\n%s", want, out)
		}
	}
}

// TestRunSchema_LegacyKey_Text_NoRefinedFields is the regression half: a legacy
// (non-refined) key's text output must not gain any refined disclosures.
func TestRunSchema_LegacyKey_Text_NoRefinedFields(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})

	if err := runSchema(f, "im.message.receive_v1", false); err != nil {
		t.Fatalf("runSchema: %v", err)
	}

	out := stdout.String()
	for _, unwanted := range []string{
		"Refined Subscription",
		"Conditional Scopes",
		"Dry Run",
		"Next Action",
	} {
		if strings.Contains(out, unwanted) {
			t.Errorf("legacy schema text unexpectedly contains %q; full output:\n%s", unwanted, out)
		}
	}
}

func TestRenderSpec_EmptySpecIsTypedInternalError(t *testing.T) {
	_, err := renderSpec(&eventlib.SchemaSpec{})
	if err == nil {
		t.Fatal("expected error for spec with neither Type nor Raw")
	}
	p, ok := errs.ProblemOf(err)
	if !ok {
		t.Fatalf("expected typed errs error, got %T: %v", err, err)
	}
	if p.Category != errs.CategoryInternal {
		t.Errorf("category = %s, want %s", p.Category, errs.CategoryInternal)
	}
}

func TestResolveSchemaJSON_InvalidBaseWithOverridesIsTypedInternalError(t *testing.T) {
	def := &eventlib.KeyDefinition{
		Key: "synthetic.invalid.base",
		Schema: eventlib.SchemaDef{
			Custom:         &eventlib.SchemaSpec{Raw: json.RawMessage("{not json")},
			FieldOverrides: map[string]schemas.FieldMeta{"x": {}},
		},
	}
	_, _, err := resolveSchemaJSON(def)
	if err == nil {
		t.Fatal("expected error for unparsable base schema")
	}
	p, ok := errs.ProblemOf(err)
	if !ok {
		t.Fatalf("expected typed errs error, got %T: %v", err, err)
	}
	if p.Category != errs.CategoryInternal {
		t.Errorf("category = %s, want %s", p.Category, errs.CategoryInternal)
	}
}
