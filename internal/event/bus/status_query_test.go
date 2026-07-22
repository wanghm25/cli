// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"github.com/larksuite/cli/internal/event/protocol"
)

// TestHandleStatusQuery_PopulatesV2Fields locks the bus-side half of the
// refined-consume startup chain's ProbeBusEligibility prereq (Task 15a,
// spec §4.2): handleStatusQuery must populate ProtocolVersion/Capabilities/
// RegisteredEventTypes so a later probe (Task 15b) can tell a new bus
// (these fields non-empty) apart from an old one (absent — spec §4.2's own
// chosen incompatibility signal). One live consumer is registered on the
// hub first so RegisteredEventTypes has something non-trivial to
// aggregate, proving it reflects the actually-registered subscriber rather
// than being hardcoded.
func TestHandleStatusQuery_PopulatesV2Fields(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	hub := NewHub()

	consumerServer, consumerClient := net.Pipe()
	defer consumerServer.Close()
	defer consumerClient.Close()
	conn := NewConn(consumerServer, nil, "im.msg", []string{"im.message.receive_v1"}, 1, "")
	hub.RegisterAndIsFirst(conn)

	b := &Bus{
		hub:       hub,
		logger:    logger,
		startTime: time.Now(),
	}

	server, client := net.Pipe()
	defer client.Close()

	go b.handleStatusQuery(server)

	line, err := protocol.ReadFrame(bufio.NewReader(client))
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	msg, err := protocol.Decode(bytes.TrimRight(line, "\n"))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	resp, ok := msg.(*protocol.StatusResponse)
	if !ok {
		t.Fatalf("decoded type = %T, want *protocol.StatusResponse", msg)
	}

	if resp.ProtocolVersion == "" {
		t.Error("ProtocolVersion is empty, want a non-empty v2 marker")
	}
	if len(resp.Capabilities) == 0 {
		t.Error("Capabilities is empty, want a non-empty capability list")
	}
	if len(resp.RegisteredEventTypes) == 0 {
		t.Fatal("RegisteredEventTypes is empty, want it to reflect the registered consumer's event types")
	}
	found := false
	for _, et := range resp.RegisteredEventTypes {
		if et == "im.message.receive_v1" {
			found = true
		}
	}
	if !found {
		t.Errorf("RegisteredEventTypes = %v, want it to contain im.message.receive_v1", resp.RegisteredEventTypes)
	}
}

// TestHandleStatusQuery_NoConsumers_RegisteredEventTypesEmptyButOtherV2FieldsSet
// covers the complementary baseline: a bus with nobody connected yet must
// still advertise ProtocolVersion/Capabilities (they describe the BUS's own
// capability, not a consumer's), while RegisteredEventTypes is correctly
// empty rather than stale/hardcoded.
func TestHandleStatusQuery_NoConsumers_RegisteredEventTypesEmptyButOtherV2FieldsSet(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	b := &Bus{
		hub:       NewHub(),
		logger:    logger,
		startTime: time.Now(),
	}

	server, client := net.Pipe()
	defer client.Close()

	go b.handleStatusQuery(server)

	line, err := protocol.ReadFrame(bufio.NewReader(client))
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	msg, err := protocol.Decode(bytes.TrimRight(line, "\n"))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	resp, ok := msg.(*protocol.StatusResponse)
	if !ok {
		t.Fatalf("decoded type = %T, want *protocol.StatusResponse", msg)
	}

	if resp.ProtocolVersion == "" {
		t.Error("ProtocolVersion is empty, want a non-empty v2 marker even with no consumers")
	}
	if len(resp.Capabilities) == 0 {
		t.Error("Capabilities is empty, want a non-empty capability list even with no consumers")
	}
	if len(resp.RegisteredEventTypes) != 0 {
		t.Errorf("RegisteredEventTypes = %v, want empty with no registered consumers", resp.RegisteredEventTypes)
	}
}
