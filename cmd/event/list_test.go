// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	eventlib "github.com/larksuite/cli/internal/event"

	_ "github.com/larksuite/cli/events"
)

func TestEventLookup_VCMeetingLifecycleKeys(t *testing.T) {
	for _, key := range []string{
		"approval.instance.status_changed_v4",
		"approval.task.status_changed_v4",
		"vc.meeting.participant_meeting_started_v1",
		"vc.meeting.participant_meeting_joined_v1",
	} {
		if _, ok := eventlib.Lookup(key); !ok {
			t.Fatalf("event.Lookup(%q) should succeed", key)
		}
	}
}

func TestRunList_TextOutput(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})

	if err := runList(f, false); err != nil {
		t.Fatalf("runList: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"KEY", "AUTH", "PARAMS", "DESCRIPTION",
		"approval.instance.status_changed_v4",
		"approval.task.status_changed_v4",
		"im.message.receive_v1",
		"im.message.message_read_v1",
		"task.task.update_user_access_v2",
		"vc.meeting.participant_meeting_started_v1",
		"vc.meeting.participant_meeting_joined_v1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("list output missing %q; full output:\n%s", want, out)
		}
	}
}

func TestRunList_JSONOutput(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})

	if err := runList(f, true); err != nil {
		t.Fatalf("runList json: %v", err)
	}

	var rows []map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &rows); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, stdout.String())
	}
	if len(rows) == 0 {
		t.Fatal("expected at least one EventKey in JSON output")
	}

	for _, row := range rows {
		for _, field := range []string{"key", "event_type", "schema"} {
			if row[field] == nil {
				t.Errorf("row missing %q: %+v", field, row)
			}
		}
	}

	gotKeys := map[string]map[string]interface{}{}
	for _, row := range rows {
		if key, ok := row["key"].(string); ok {
			gotKeys[key] = row
		}
	}
	var foundTask bool
	for key, row := range gotKeys {
		if key == "task.task.update_user_access_v2" {
			foundTask = true
			if row["single_consumer"] != true {
				t.Errorf("task row single_consumer = %v, want true", row["single_consumer"])
			}
		}
	}
	if !foundTask {
		t.Fatal("event list JSON missing task.task.update_user_access_v2")
	}
	for _, want := range []string{
		"approval.instance.status_changed_v4",
		"approval.task.status_changed_v4",
		"vc.meeting.participant_meeting_started_v1",
		"vc.meeting.participant_meeting_joined_v1",
	} {
		if _, ok := gotKeys[want]; !ok {
			t.Errorf("JSON list output missing %q", want)
		}
	}
}

// TestListJSON_RefinedSubscriptionAdditiveFields pins that for a
// RefinedSubscription key, `event list --json` additively emits
// refined_subscription/key_templates[]/auth_types/dry_run_supported/next_action
// on top of the existing resolved_output_schema.
func TestListJSON_RefinedSubscriptionAdditiveFields(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})

	if err := runList(f, true); err != nil {
		t.Fatalf("runList json: %v", err)
	}

	var rows []map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &rows); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, stdout.String())
	}

	gotKeys := map[string]map[string]interface{}{}
	for _, row := range rows {
		if key, ok := row["key"].(string); ok {
			gotKeys[key] = row
		}
	}

	refined, ok := gotKeys["im.message.created_v1"]
	if !ok {
		t.Fatal("event list JSON missing im.message.created_v1")
	}

	if refined["refined_subscription"] != true {
		t.Errorf("im.message.created_v1 refined_subscription = %v, want true", refined["refined_subscription"])
	}
	if refined["dry_run_supported"] != true {
		t.Errorf("im.message.created_v1 dry_run_supported = %v, want true", refined["dry_run_supported"])
	}
	wantNextAction := "run `lark-cli event schema im.message.created_v1 --json` before consume"
	if refined["next_action"] != wantNextAction {
		t.Errorf("im.message.created_v1 next_action = %v, want %q", refined["next_action"], wantNextAction)
	}
	if refined["resolved_output_schema"] == nil {
		t.Error("im.message.created_v1 resolved_output_schema must be preserved")
	}

	gotTopAuth, ok := refined["auth_types"].([]interface{})
	if !ok || len(gotTopAuth) != 2 || gotTopAuth[0] != "user" || gotTopAuth[1] != "bot" {
		t.Errorf("im.message.created_v1 auth_types = %v, want [user bot]", refined["auth_types"])
	}

	templatesRaw, ok := refined["key_templates"].([]interface{})
	if !ok {
		t.Fatalf("im.message.created_v1 key_templates missing or wrong type: %v (%T)", refined["key_templates"], refined["key_templates"])
	}
	wantTemplates := []struct {
		template, example, description string
		authTypes                      []string
	}{
		{
			template:    "im.message.created_v1/chat-id/{chat_id}",
			example:     "im.message.created_v1/chat-id/oc_9f3b1c2d8a",
			description: "监听指定会话内的新消息",
			authTypes:   []string{"user", "bot"},
		},
		{
			template:    "im.message.created_v1/owner/me",
			example:     "im.message.created_v1/owner/me",
			description: "监听当前登录用户可见的新消息；me 是固定值，按订阅身份解析",
			authTypes:   []string{"user"},
		},
	}
	if len(templatesRaw) != len(wantTemplates) {
		t.Fatalf("im.message.created_v1 key_templates length = %d, want %d", len(templatesRaw), len(wantTemplates))
	}
	for i, want := range wantTemplates {
		tmpl, ok := templatesRaw[i].(map[string]interface{})
		if !ok {
			t.Fatalf("key_templates[%d] wrong type: %T", i, templatesRaw[i])
		}
		if tmpl["template"] != want.template {
			t.Errorf("key_templates[%d].template = %v, want %q", i, tmpl["template"], want.template)
		}
		if tmpl["example"] != want.example {
			t.Errorf("key_templates[%d].example = %v, want %q", i, tmpl["example"], want.example)
		}
		if tmpl["description"] != want.description {
			t.Errorf("key_templates[%d].description = %v, want %q", i, tmpl["description"], want.description)
		}
		gotAuth, ok := tmpl["auth_types"].([]interface{})
		if !ok || len(gotAuth) != len(want.authTypes) {
			t.Fatalf("key_templates[%d].auth_types = %v, want %v", i, tmpl["auth_types"], want.authTypes)
		}
		for j, a := range want.authTypes {
			if gotAuth[j] != a {
				t.Errorf("key_templates[%d].auth_types[%d] = %v, want %q", i, j, gotAuth[j], a)
			}
		}
	}
}

// TestListJSON_LegacyKeyUnchanged is the regression half: a
// non-refined (legacy) EventKey must not gain any of the new refined-only
// fields in `event list --json` output.
func TestListJSON_LegacyKeyUnchanged(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "test"})

	if err := runList(f, true); err != nil {
		t.Fatalf("runList json: %v", err)
	}

	var rows []map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &rows); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, stdout.String())
	}

	var legacy map[string]interface{}
	for _, row := range rows {
		if row["key"] == "im.message.receive_v1" {
			legacy = row
			break
		}
	}
	if legacy == nil {
		t.Fatal("event list JSON missing im.message.receive_v1")
	}

	for _, field := range []string{"refined_subscription", "key_templates", "resource_type", "dry_run_supported", "next_action"} {
		if v, present := legacy[field]; present {
			t.Errorf("legacy key im.message.receive_v1 must not have %q field; got %v", field, v)
		}
	}
	for _, field := range []string{"key", "event_type", "schema", "resolved_output_schema", "auth_types"} {
		if _, present := legacy[field]; !present {
			t.Errorf("legacy key im.message.receive_v1 unexpectedly lost %q field", field)
		}
	}
}
