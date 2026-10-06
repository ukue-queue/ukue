// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package main

import (
	"os/exec"
	"syscall"
)

// setProcessGroup starts the command in its own process group, so it and
// anything it starts can be stopped together. It also keeps a Ctrl-C in the
// terminal away from the command: ukue decides when commands stop.
func setProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// terminateProcessGroup asks the command's process group to stop.
func terminateProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM); err != nil {
		return cmd.Process.Signal(syscall.SIGTERM)
	}
	return nil
}

// killProcessGroup kills whatever is left of the command's process group.
func killProcessGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
