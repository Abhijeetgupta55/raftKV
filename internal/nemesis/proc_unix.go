//go:build !windows

package nemesis

import "syscall"

func suspendProcess(pid int) error { return syscall.Kill(pid, syscall.SIGSTOP) }
func resumeProcess(pid int) error  { return syscall.Kill(pid, syscall.SIGCONT) }
