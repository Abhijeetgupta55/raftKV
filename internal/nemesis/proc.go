package nemesis

import (
	"fmt"
	"io"
	"os"
	"os/exec"
)

// Proc is one node's OS process under nemesis control.
type Proc struct {
	Name string
	cmd  *exec.Cmd
}

// StartProc launches bin with args. Output goes to logTo when given —
// per-node log files are the post-mortem evidence when a scenario fails —
// and to the test's stdout/stderr otherwise. extraEnv entries ("K=V") are
// appended to the inherited environment (the mutation check uses this).
func StartProc(name string, logTo io.Writer, extraEnv []string, bin string, args ...string) (*Proc, error) {
	cmd := exec.Command(bin, args...)
	if logTo != nil {
		cmd.Stdout, cmd.Stderr = logTo, logTo
	} else {
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	}
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &Proc{Name: name, cmd: cmd}, nil
}

// Kill is kill -9: no shutdown handler runs, no fsync gets a second
// chance. Idempotent.
func (p *Proc) Kill() {
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
		_, _ = p.cmd.Process.Wait()
	}
}

// Suspend freezes every thread of the process (SIGSTOP on unix, the
// NtSuspendProcess debug API on Windows): the process keeps its sockets
// and its belief that it is leader — the zombie fault.
func (p *Proc) Suspend() error {
	if p.cmd == nil || p.cmd.Process == nil {
		return fmt.Errorf("nemesis: %s not running", p.Name)
	}
	return suspendProcess(p.cmd.Process.Pid)
}

// Resume thaws a suspended process (SIGCONT / NtResumeProcess).
func (p *Proc) Resume() error {
	if p.cmd == nil || p.cmd.Process == nil {
		return fmt.Errorf("nemesis: %s not running", p.Name)
	}
	return resumeProcess(p.cmd.Process.Pid)
}
