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

// terminateProcessGroup stops the command. Windows has no SIGTERM, so the
// command is ended at once; processes it started itself keep running.
func terminateProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}

func killProcessGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}
