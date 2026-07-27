// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package imcontract

import "github.com/spf13/cobra"

const (
	helpContractAnnotation = "imcontract.help.contract-key"
)

func AnnotateHelpContract(cmd *cobra.Command, key ContractKey) {
	if cmd == nil || key == "" {
		return
	}
	if cmd.Annotations == nil {
		cmd.Annotations = map[string]string{}
	}
	cmd.Annotations[helpContractAnnotation] = string(key)
}

func HelpText(cmd *cobra.Command) string {
	if cmd == nil || !cmd.Runnable() || cmd.Annotations == nil {
		return ""
	}
	contract, ok := Lookup(ContractKey(cmd.Annotations[helpContractAnnotation]))
	if !ok {
		return ""
	}
	return contract.HelpPolicy.Text()
}
