// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package main

import (
	"os/exec"
	"syscall"
)

// setProcessGroup starts the command in its own process group, so stopping
// it also stops anything it started.
func setProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
		return cmd.Process.Kill()
	}
	return nil
}
