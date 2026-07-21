package refined

import (
	"reflect"
	"testing"
)

// TestRefinedKeys pins the content of the im.message.created_v1 mock entry
// (events/refined/refined_keys_mock.json) to design spec §2.2. This mock is
// the contract the refined-key parser (a later task) consumes, so a wrong
// template string / selector / auth_types here must fail loudly rather than
// pass silently.
func TestRefinedKeys(t *testing.T) {
	var found bool
	for _, k := range Keys() {
		if k.Key != "im.message.created_v1" {
			continue
		}
		found = true

		if !k.RefinedSubscription {
			t.Errorf("RefinedSubscription = false, want true")
		}
		if k.ResourceType != "im.message" {
			t.Errorf("ResourceType = %q, want %q", k.ResourceType, "im.message")
		}
		if want := []string{"user", "bot"}; !reflect.DeepEqual(k.AuthTypes, want) {
			t.Errorf("AuthTypes = %v, want %v", k.AuthTypes, want)
		}
		if len(k.KeyTemplates) != 2 {
			t.Fatalf("KeyTemplates = %+v, want 2 entries", k.KeyTemplates)
		}

		chatID := k.KeyTemplates[0]
		if want := "im.message.created_v1/chat-id/{chat_id}"; chatID.Template != want {
			t.Errorf("KeyTemplates[0].Template = %q, want %q", chatID.Template, want)
		}
		if chatID.PathSegment != "chat-id" {
			t.Errorf("KeyTemplates[0].PathSegment = %q, want %q", chatID.PathSegment, "chat-id")
		}
		if chatID.SelectorKey != "chat_id" {
			t.Errorf("KeyTemplates[0].SelectorKey = %q, want %q", chatID.SelectorKey, "chat_id")
		}
		if chatID.FixedValue != "" {
			t.Errorf("KeyTemplates[0].FixedValue = %q, want empty", chatID.FixedValue)
		}
		if want := []string{"user", "bot"}; !reflect.DeepEqual(chatID.AuthTypes, want) {
			t.Errorf("KeyTemplates[0].AuthTypes = %v, want %v", chatID.AuthTypes, want)
		}

		owner := k.KeyTemplates[1]
		if want := "im.message.created_v1/owner/me"; owner.Template != want {
			t.Errorf("KeyTemplates[1].Template = %q, want %q", owner.Template, want)
		}
		if owner.PathSegment != "owner" {
			t.Errorf("KeyTemplates[1].PathSegment = %q, want %q", owner.PathSegment, "owner")
		}
		if owner.SelectorKey != "owner" {
			t.Errorf("KeyTemplates[1].SelectorKey = %q, want %q", owner.SelectorKey, "owner")
		}
		if owner.FixedValue != "me" {
			t.Errorf("KeyTemplates[1].FixedValue = %q, want %q", owner.FixedValue, "me")
		}
		if want := []string{"user"}; !reflect.DeepEqual(owner.AuthTypes, want) {
			t.Errorf("KeyTemplates[1].AuthTypes = %v, want %v", owner.AuthTypes, want)
		}
	}
	if !found {
		t.Fatal("mock refined key im.message.created_v1 not in Keys()")
	}
}
