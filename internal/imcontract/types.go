// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package imcontract evaluates IM command completion evidence.
package imcontract

import "time"

type ContractKey string

type StrategyKind string

const (
	EntityReadKind                 StrategyKind = "entity_read"
	CollectionReadKind             StrategyKind = "collection_read"
	SearchReadKind                 StrategyKind = "search_read"
	MaterializeReadKind            StrategyKind = "materialize_read"
	AuthoritativeAckKind           StrategyKind = "authoritative_ack"
	RequiredResultKind             StrategyKind = "required_result"
	BatchPartialKind               StrategyKind = "batch_partial"
	RequiredResultBatchPartialKind StrategyKind = "required_result_batch_partial"
	ResponseSetAssertionKind       StrategyKind = "response_set_assertion"
	ExemptionKind                  StrategyKind = "exemption"
)

func (k StrategyKind) IsWrite() bool {
	switch k {
	case AuthoritativeAckKind, RequiredResultKind, BatchPartialKind,
		RequiredResultBatchPartialKind, ResponseSetAssertionKind, ExemptionKind:
		return true
	default:
		return false
	}
}

func (k StrategyKind) IsRead() bool {
	switch k {
	case EntityReadKind, CollectionReadKind, SearchReadKind, MaterializeReadKind:
		return true
	default:
		return false
	}
}

type ReplayMode string

const (
	ReplayForbidden          ReplayMode = "forbidden"
	ReplaySafe               ReplayMode = "safe"
	ReplaySameIdempotencyKey ReplayMode = "same_idempotency_key"
)

type AssertionMode string

const (
	AssertRequestedPresent AssertionMode = "requested_present"
	AssertRequestedAbsent  AssertionMode = "requested_absent"
)

type requiredShape uint8

const (
	requiredTopString requiredShape = iota + 1
	requiredTopObject
	requiredNestedString
)

type evidenceShape uint8

const (
	evidenceStrings evidenceShape = iota + 1
	evidenceObjects
	evidenceNestedObjects
	evidenceFeedObjects
	evidenceNestedFeedObjects
	evidenceStatusObjects
)

type requiredSpec struct {
	shape requiredShape
	field string
	child string
}

type evidenceSpec struct {
	shape     evidenceShape
	field     string
	idField   string
	container string
}

type Strategy struct {
	Kind         StrategyKind
	required     requiredSpec
	request      evidenceSpec
	failures     []evidenceSpec
	pending      []evidenceSpec
	responseSets []evidenceSpec
	assertion    AssertionMode
	resultLedger *evidenceSpec
	// collectionField is only used by the two fixed IM search strategies to
	// determine whether an exhausted search returned no candidates. It is not
	// a general response path or field extractor.
	collectionField string
	readHint        string
}

type HelpPolicy string

type Exemption struct {
	Reason string
	Owner  string
	Expiry string
}

func (e Exemption) ExpiryTime() (time.Time, error) {
	return time.Parse("2006-01-02", e.Expiry)
}

type Contract struct {
	Key        ContractKey
	Strategy   Strategy
	ReplayMode ReplayMode
	HelpPolicy HelpPolicy
	Exemption  *Exemption
}

type FactKind string

const (
	FactMediaPreuploadPerformed FactKind = "media_preupload_performed"
	FactFlagFeedLayerPending    FactKind = "flag_feed_layer_pending"
	FactWriteAttempted          FactKind = "write_attempted"
)

type Fact struct {
	Kind FactKind
	Item string
}

type Result struct {
	OK       bool
	Data     any
	Hint     string
	ExitCode int
}
