//go:build darwin || linux

package conpty

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// runTestSupervisor mirrors `ao agent-process supervise`'s process shape: it
// owns the managed agent as its only child and stays alive after that child
// exits until its exit report completes. Here the report is a file barrier the
// test controls, so the window is ordered without guessing timing.
func runTestSupervisor(args []string) int {
	for len(args) > 0 && args[0] != "--" {
		args = args[1:]
	}
	if len(args) < 2 {
		return 2
	}
	child := exec.Command(args[1], args[2:]...) //nolint:forbidigo // test stand-in for the AO supervisor
	child.Stdin, child.Stdout, child.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := child.Start(); err != nil {
		return 1
	}
	_ = child.Wait()
	_ = os.WriteFile(os.Getenv("AO_TEST_SUPERVISOR_REPORTING"), []byte("reporting"), 0o600)
	for {
		if _, err := os.Stat(os.Getenv("AO_TEST_SUPERVISOR_RELEASE")); err == nil {
			return 0
		}
		time.Sleep(10 * time.Millisecond)
	}
}

const supervisedWindowAgent = `printf ready > "$AO_TEST_AGENT_READY"
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$AO_TEST_AGENT_INPUT"
  if [ "$line" = "__exit__" ]; then exit 0; fi
done`

type supervisedWindow struct {
	rt        *Runtime
	handle    ports.RuntimeHandle
	ref       ports.FencedRuntimeRef
	exact     ports.SupervisedProcessRef
	input     string
	reporting string
	release   string
}

func startSupervisedWindow(t *testing.T, sessionID, launchID string, supervised bool) *supervisedWindow {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	w := &supervisedWindow{
		rt:        New(Options{RunFilePath: filepath.Join(dir, "running.json")}),
		input:     filepath.Join(dir, "agent.input"),
		reporting: filepath.Join(dir, "supervisor.reporting"),
		release:   filepath.Join(dir, "supervisor.release"),
	}
	argv := []string{"/bin/sh", "-c", supervisedWindowAgent}
	if supervised {
		argv = append([]string{exe, "agent-process", "supervise", "--session", sessionID, "--launch", launchID, "--"}, argv...)
	}
	ready := filepath.Join(dir, "agent.ready")
	w.handle, err = w.rt.Create(context.Background(), ports.RuntimeConfig{
		SessionID: domain.SessionID(sessionID), WorkspacePath: dir, Argv: argv,
		Env: map[string]string{
			runtimeLaunchIDEnv:             launchID,
			"AO_TEST_AGENT_READY":          ready,
			"AO_TEST_AGENT_INPUT":          w.input,
			"AO_TEST_SUPERVISOR_REPORTING": w.reporting,
			"AO_TEST_SUPERVISOR_RELEASE":   w.release,
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() {
		_ = os.WriteFile(w.release, nil, 0o600)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = w.rt.Destroy(ctx, w.handle)
	})
	w.ref = ports.FencedRuntimeRef{Handle: w.handle, SessionID: domain.SessionID(sessionID), Generation: launchID}
	w.exact = ports.SupervisedProcessRef{SessionID: domain.SessionID(sessionID), LaunchID: launchID}
	waitForTestFile(t, ready, "ready")
	return w
}

func (w *supervisedWindow) exitAgent(t *testing.T) {
	t.Helper()
	if err := w.rt.SendInput(context.Background(), w.handle, "__exit__\r"); err != nil {
		t.Fatal(err)
	}
	waitForTestFile(t, w.input, "__exit__")
}

func waitForTestFile(t *testing.T, path, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		raw, _ := os.ReadFile(path)
		if strings.Contains(string(raw), want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %q in %s; got %q", want, path, raw)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// T02 adapter fixture: real native host and PTY, with the supervisor held in
// its exit-report window after the managed agent exits.
func TestExactProbeRejectsSupervisorReportingExitedChild(t *testing.T) {
	w := startSupervisedWindow(t, "t02-fixture", "launch-1", true)
	ctx := context.Background()
	if probe := w.rt.ProbeFencedRuntime(ctx, w.ref); probe.Liveness != ports.FencedAlive {
		t.Fatalf("live supervised agent = %+v, want alive", probe)
	}
	if alive, err := w.rt.IsExactSupervisedProcessAlive(ctx, w.handle, w.exact); err != nil || !alive {
		t.Fatalf("live supervised agent exact = %v, %v; want alive", alive, err)
	}

	w.exitAgent(t)
	waitForTestFile(t, w.reporting, "reporting")
	if alive, err := w.rt.IsChildAlive(ctx, w.handle); err != nil || !alive {
		t.Fatalf("precondition: supervisor must be alive while reporting: %v, %v", alive, err)
	}
	// The ordinary reaper probe intentionally keeps reporting the supervisor.
	if alive, err := w.rt.IsSupervisedProcessAlive(ctx, w.handle, w.exact); err != nil || !alive {
		t.Fatalf("ordinary supervised probe = %v, %v; want unchanged alive", alive, err)
	}
	if probe := w.rt.ProbeFencedRuntime(ctx, w.ref); probe.Liveness != ports.FencedDead || probe.Reason != ports.FencedReasonExactAbsent {
		t.Fatalf("fenced probe during report window = %+v, want dead/exact_absent", probe)
	}
	if alive, err := w.rt.IsExactSupervisedProcessAlive(ctx, w.handle, w.exact); err != nil || alive {
		t.Fatalf("exact probe during report window = %v, %v; want definitely not alive", alive, err)
	}
	// A different generation is never confirmed and never reported dead.
	if probe := w.rt.ProbeFencedRuntime(ctx, ports.FencedRuntimeRef{Handle: w.handle, SessionID: w.ref.SessionID, Generation: "launch-0"}); probe.Liveness != ports.FencedUnknown || probe.Reason != ports.FencedReasonGenerationMismatch {
		t.Fatalf("old generation probe = %+v, want unknown/generation_mismatch", probe)
	}
}

// An unsupervised hook-native agent is the host's direct child, so its own
// status remains the exact evidence; no process-table inspection applies.
func TestExactProbeUsesDirectChildForUnsupervisedLaunch(t *testing.T) {
	w := startSupervisedWindow(t, "t02-direct", "launch-1", false)
	ctx := context.Background()
	if probe := w.rt.ProbeFencedRuntime(ctx, w.ref); probe.Liveness != ports.FencedAlive {
		t.Fatalf("live direct agent = %+v, want alive", probe)
	}
	w.exitAgent(t)
	deadline := time.Now().Add(5 * time.Second)
	for {
		probe := w.rt.ProbeFencedRuntime(ctx, w.ref)
		if probe.Liveness == ports.FencedDead {
			break
		}
		if probe.Liveness == ports.FencedAlive && time.Now().After(deadline) {
			t.Fatalf("direct agent exit never observed: %+v", probe)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
