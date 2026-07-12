//go:build windows

package nemesis

import (
	"fmt"
	"syscall"
)

// Windows has no SIGSTOP; the honest equivalent — sanctioned by the plan —
// is the kernel debug API: NtSuspendProcess/NtResumeProcess freeze and
// thaw every thread, exactly what a debugger's "pause" does. The process
// keeps its sockets, its memory, and its belief about its role.

var (
	ntdll            = syscall.NewLazyDLL("ntdll.dll")
	ntSuspendProcess = ntdll.NewProc("NtSuspendProcess")
	ntResumeProcess  = ntdll.NewProc("NtResumeProcess")
)

const processSuspendResume = 0x0800

func withProcessHandle(pid int, fn func(h syscall.Handle) error) error {
	h, err := syscall.OpenProcess(processSuspendResume, false, uint32(pid))
	if err != nil {
		return fmt.Errorf("nemesis: open pid %d: %w", pid, err)
	}
	defer syscall.CloseHandle(h)
	return fn(h)
}

func suspendProcess(pid int) error {
	return withProcessHandle(pid, func(h syscall.Handle) error {
		if rc, _, _ := ntSuspendProcess.Call(uintptr(h)); rc != 0 {
			return fmt.Errorf("nemesis: NtSuspendProcess(%d) = %#x", pid, rc)
		}
		return nil
	})
}

func resumeProcess(pid int) error {
	return withProcessHandle(pid, func(h syscall.Handle) error {
		if rc, _, _ := ntResumeProcess.Call(uintptr(h)); rc != 0 {
			return fmt.Errorf("nemesis: NtResumeProcess(%d) = %#x", pid, rc)
		}
		return nil
	})
}
