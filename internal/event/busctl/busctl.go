// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package busctl is the wire-level control client for the event bus daemon.
package busctl

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/larksuite/cli/internal/event/protocol"
	"github.com/larksuite/cli/internal/event/transport"
)

const readTimeout = 5 * time.Second // matches protocol.WriteTimeout

// ErrStatusUnverified marks every QueryStatus failure that happens AFTER
// Dial already succeeded — sending the status query, reading the response,
// decoding it, or the response not being a StatusResponse at all — as
// opposed to Dial itself failing, which means no local bus is listening at
// all. A caller that needs to react differently to these two cases (a local
// bus that IS running but whose status could not be confirmed, versus no
// local bus reachable) should use errors.Is against this sentinel rather
// than treating every QueryStatus error alike.
var ErrStatusUnverified = errors.New("busctl: local bus dialed but its status could not be verified")

func QueryStatus(tr transport.IPC, appID string) (*protocol.StatusResponse, error) {
	conn, err := tr.Dial(tr.Address(appID))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := protocol.EncodeWithDeadline(conn, protocol.NewStatusQuery(), protocol.WriteTimeout); err != nil {
		return nil, fmt.Errorf("%w: send status query: %w", ErrStatusUnverified, err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		return nil, fmt.Errorf("%w: set read deadline: %w", ErrStatusUnverified, err)
	}
	line, err := protocol.ReadFrame(bufio.NewReader(conn))
	if err != nil {
		return nil, fmt.Errorf("%w: read status response: %w", ErrStatusUnverified, err)
	}

	msg, err := protocol.Decode(bytes.TrimRight(line, "\n"))
	if err != nil {
		return nil, fmt.Errorf("%w: decode status response: %w", ErrStatusUnverified, err)
	}
	resp, ok := msg.(*protocol.StatusResponse)
	if !ok {
		return nil, fmt.Errorf("%w: unexpected response type from bus: %T", ErrStatusUnverified, msg)
	}
	return resp, nil
}

// SendShutdown sends a Shutdown command; caller polls Dial to confirm exit.
func SendShutdown(tr transport.IPC, appID string) error {
	conn, err := tr.Dial(tr.Address(appID))
	if err != nil {
		return err
	}
	defer conn.Close()
	return protocol.EncodeWithDeadline(conn, protocol.NewShutdown(), protocol.WriteTimeout)
}

// SendSubscriptionUpdated tells appID's bus that remoteSubscriptionID was just
// updated by an operator, so it can proactively degrade the matching local
// consumers. Fire-and-forget, exactly like SendShutdown: it writes the frame
// and returns without reading a response — the caller treats any error as
// best-effort (the platform's own updated_v1 remains the backstop).
func SendSubscriptionUpdated(tr transport.IPC, appID, remoteSubscriptionID string) error {
	conn, err := tr.Dial(tr.Address(appID))
	if err != nil {
		return err
	}
	defer conn.Close()
	return protocol.EncodeWithDeadline(conn, protocol.NewSubscriptionUpdated(remoteSubscriptionID), protocol.WriteTimeout)
}
