// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package main

import (
	"os/exec"
	"syscall"
)

// setProcessGroup starts the command in a new process group.
func setProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

func killProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}
