// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package imcontract

import (
	"fmt"
	"sort"
	"time"
)

func ack(key string) Contract {
	return Contract{Key: ContractKey(key), Strategy: Strategy{Kind: AuthoritativeAckKind}, ReplayMode: ReplayForbidden}
}

func required(key string, result requiredSpec, replay ReplayMode) Contract {
	return Contract{
		Key:        ContractKey(key),
		Strategy:   Strategy{Kind: RequiredResultKind, required: result},
		ReplayMode: replay,
	}
}

func batch(key string, request evidenceSpec, failures ...evidenceSpec) Contract {
	return Contract{
		Key: ContractKey(key),
		Strategy: Strategy{
			Kind:     BatchPartialKind,
			request:  request,
			failures: failures,
		},
		ReplayMode: ReplayForbidden,
	}
}

func read(key string, kind StrategyKind) Contract {
	return Contract{
		Key:      ContractKey(key),
		Strategy: Strategy{Kind: kind},
	}
}

func search(key, collectionField string) Contract {
	return Contract{
		Key: ContractKey(key),
		Strategy: Strategy{
			Kind:            SearchReadKind,
			collectionField: collectionField,
		},
	}
}

func topString(field string) requiredSpec {
	return requiredSpec{shape: requiredTopString, field: field}
}

func topObject(field string) requiredSpec {
	return requiredSpec{shape: requiredTopObject, field: field}
}

func nestedString(field, child string) requiredSpec {
	return requiredSpec{shape: requiredNestedString, field: field, child: child}
}

func stringsFrom(field string) evidenceSpec {
	return evidenceSpec{shape: evidenceStrings, field: field}
}

func objectsFrom(field, idField string) evidenceSpec {
	return evidenceSpec{shape: evidenceObjects, field: field, idField: idField}
}

func nestedObjectsFrom(field, container, idField string) evidenceSpec {
	return evidenceSpec{
		shape: evidenceNestedObjects, field: field, container: container, idField: idField,
	}
}

func feedObjectsFrom(field string) evidenceSpec {
	return evidenceSpec{shape: evidenceFeedObjects, field: field}
}

func nestedFeedObjectsFrom(field, container string) evidenceSpec {
	return evidenceSpec{shape: evidenceNestedFeedObjects, field: field, container: container}
}

func statusObjectsFrom(field, idField string) evidenceSpec {
	return evidenceSpec{shape: evidenceStatusObjects, field: field, idField: idField}
}

var contracts = buildContracts()

func buildContracts() map[ContractKey]Contract {
	all := []Contract{
		read("im +feed-group-query-item", EntityReadKind),
		read("im +messages-mget", EntityReadKind),
		read("im chat.nickname get", EntityReadKind),
		read("im chat.user_setting batch_query", EntityReadKind),
		read("im chats get", EntityReadKind),
		read("im feed.groups batch_query", EntityReadKind),
		func() Contract {
			c := read("im reactions batch_query", EntityReadKind)
			c.Strategy.readHint = hintBatchReactions
			return c
		}(),

		read("im +chat-list", CollectionReadKind),
		read("im +chat-members-list", CollectionReadKind),
		read("im +chat-messages-list", CollectionReadKind),
		read("im +feed-group-list", CollectionReadKind),
		read("im +feed-group-list-item", CollectionReadKind),
		read("im +feed-shortcut-list", CollectionReadKind),
		read("im +flag-list", CollectionReadKind),
		read("im +threads-messages-list", CollectionReadKind),
		read("im chat.members bots", CollectionReadKind),
		read("im chat.members get", CollectionReadKind),
		read("im chat.moderation get", CollectionReadKind),
		read("im messages read_users", CollectionReadKind),
		read("im pins list", CollectionReadKind),
		read("im reactions list", CollectionReadKind),

		search("im +chat-search", "chats"),
		search("im +messages-search", "messages"),

		read("im +messages-resources-download", MaterializeReadKind),

		ack("im +chat-update"),
		ack("im +flag-create"),
		ack("im chat.nickname delete"),
		ack("im chat.nickname update"),
		ack("im chats update"),
		ack("im feed.groups delete"),
		ack("im feed.groups update"),
		ack("im messages delete"),
		ack("im pins delete"),

		required("im +chat-create", topString("chat_id"), ReplayForbidden),
		required("im +messages-reply", topString("message_id"), ReplaySameIdempotencyKey),
		required("im +messages-send", topString("message_id"), ReplaySameIdempotencyKey),
		required("im chats create", topString("chat_id"), ReplaySameIdempotencyKey),
		required("im chats link", topString("share_link"), ReplayForbidden),
		required("im feed.groups create", topString("group_id"), ReplayForbidden),
		required("im images create", topString("image_key"), ReplayForbidden),
		required("im messages forward", topString("message_id"), ReplaySameIdempotencyKey),
		required("im pins create", topObject("pin"), ReplayForbidden),
		required("im reactions create", topString("reaction_id"), ReplayForbidden),
		required("im reactions delete", topString("reaction_id"), ReplayForbidden),
		required("im threads forward", topString("message_id"), ReplaySameIdempotencyKey),

		func() Contract {
			c := batch(
				"im +feed-shortcut-create",
				objectsFrom("shortcuts", "feed_card_id"),
				nestedObjectsFrom("failed_shortcuts", "shortcut", "feed_card_id"),
			)
			c.ReplayMode = ReplaySafe
			return c
		}(),
		batch(
			"im +feed-shortcut-remove",
			objectsFrom("shortcuts", "feed_card_id"),
			nestedObjectsFrom("failed_shortcuts", "shortcut", "feed_card_id"),
		),
		{
			Key: "im +flag-cancel",
			Strategy: Strategy{
				Kind:         BatchPartialKind,
				resultLedger: ptrEvidence(statusObjectsFrom("results", "flag_type")),
			},
			ReplayMode: ReplaySafe,
		},
		{
			Key: "im chat.members create",
			Strategy: Strategy{
				Kind:    BatchPartialKind,
				request: stringsFrom("id_list"),
				failures: []evidenceSpec{
					stringsFrom("invalid_id_list"),
					stringsFrom("not_existed_id_list"),
				},
				pending: []evidenceSpec{stringsFrom("pending_approval_id_list")},
			},
			ReplayMode: ReplayForbidden,
		},
		batch("im chat.members delete", stringsFrom("id_list"), stringsFrom("invalid_id_list")),
		batch(
			"im chat.user_setting batch_update",
			objectsFrom("chat_settings", "chat_id"),
			objectsFrom("invalid_ids", "id"),
		),
		{
			Key: "im feed.groups batch_add_item",
			Strategy: Strategy{
				Kind:     BatchPartialKind,
				request:  feedObjectsFrom("items"),
				failures: []evidenceSpec{nestedFeedObjectsFrom("failed_items", "item")},
			},
			ReplayMode: ReplayForbidden,
		},
		{
			Key: "im feed.groups batch_remove_item",
			Strategy: Strategy{
				Kind:     BatchPartialKind,
				request:  feedObjectsFrom("items"),
				failures: []evidenceSpec{nestedFeedObjectsFrom("failed_items", "item")},
			},
			ReplayMode: ReplayForbidden,
		},
		batch("im messages urgent_app", stringsFrom("user_id_list"), stringsFrom("invalid_user_id_list")),
		batch("im messages urgent_phone", stringsFrom("user_id_list"), stringsFrom("invalid_user_id_list")),
		batch("im messages urgent_sms", stringsFrom("user_id_list"), stringsFrom("invalid_user_id_list")),
		{
			Key: "im messages merge_forward",
			Strategy: Strategy{
				Kind:     RequiredResultBatchPartialKind,
				required: nestedString("message", "message_id"),
				request:  stringsFrom("message_id_list"),
				failures: []evidenceSpec{stringsFrom("invalid_message_id_list")},
			},
			ReplayMode: ReplaySameIdempotencyKey,
		},
		{
			Key: "im chat.managers add_managers",
			Strategy: Strategy{
				Kind:         ResponseSetAssertionKind,
				request:      stringsFrom("manager_ids"),
				responseSets: []evidenceSpec{stringsFrom("chat_managers"), stringsFrom("chat_bot_managers")},
				assertion:    AssertRequestedPresent,
			},
			ReplayMode: ReplayForbidden,
		},
		{
			Key: "im chat.managers delete_managers",
			Strategy: Strategy{
				Kind:         ResponseSetAssertionKind,
				request:      stringsFrom("manager_ids"),
				responseSets: []evidenceSpec{stringsFrom("chat_managers"), stringsFrom("chat_bot_managers")},
				assertion:    AssertRequestedAbsent,
			},
			ReplayMode: ReplayForbidden,
		},
		{
			Key:        "im chat.moderation update",
			Strategy:   Strategy{Kind: ExemptionKind},
			ReplayMode: ReplayForbidden,
			Exemption: &Exemption{
				Reason: "OpenAPI lacks per-item results",
				Owner:  "IM backend",
				Expiry: "2026-10-25",
			},
		},
	}
	out := make(map[ContractKey]Contract, len(all))
	for _, c := range all {
		out[c.Key] = c
	}
	return out
}

func ptrEvidence(spec evidenceSpec) *evidenceSpec {
	return &spec
}

func Lookup(key ContractKey) (Contract, bool) {
	c, ok := contracts[key]
	return c, ok
}

func All() []Contract {
	out := make([]Contract, 0, len(contracts))
	for _, c := range contracts {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func ValidateRegistry(now time.Time) error {
	for key, c := range contracts {
		if key == "" || c.Strategy.Kind == "" {
			return fmt.Errorf("invalid IM contract %q", key)
		}
		if c.Exemption == nil {
			continue
		}
		expiry, err := c.Exemption.ExpiryTime()
		if err != nil {
			return fmt.Errorf("invalid exemption expiry for %q: %w", key, err)
		}
		if now.After(expiry.Add(24 * time.Hour)) {
			return fmt.Errorf("IM contract exemption expired for %q (owner: %s)", key, c.Exemption.Owner)
		}
	}
	return nil
}
