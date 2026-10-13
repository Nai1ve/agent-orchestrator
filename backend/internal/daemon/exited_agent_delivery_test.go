//go:build darwin || linux

package daemon

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/runtime/conpty"
	telemetryadapter "github.com/aoagents/agent-orchestrator/backend/internal/adapters/telemetry"
	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	"github.com/aoagents/agent-orchestrator/backend/internal/lifecycle"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	sessionsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/session"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
)

// nativeTestAgent stands in for a hook-native TUI agent running directly on a
// real native PTY host. It logs every line it actually reads, which is the
// independent receipt evidence: a successful host write is not.
const nativeTestAgent = `printf ready > "$AO_TEST_AGENT_READY"
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$AO_TEST_AGENT_INPUT"
  if [ "$line" = "__exit__" ]; then exit 0; fi
done`

type nativeDeliveryFixture struct {
	dir      string
	store    *sqlite.Store
	runtime  *conpty.Runtime
	lcm      *lifecycle.Manager
	sessMgr  sessionLifecycle
	agents   ports.AgentResolver
	sessions *sessionsvc.Service
	id       domain.SessionID
}

// newNativeDeliveryFixture wires lifecycle, the mode-aware lifecycle messenger,
// Session Manager, and the runtime messenger exactly as daemon boot does, over
// SQLite and a real native PTY runtime. The reaper is deliberately not started:
// these tests hold the window in which the durable row still says idle after
// the agent process has exited.
func newNativeDeliveryFixture(t *testing.T) *nativeDeliveryFixture {
	t.Helper()
	dir := t.TempDir()
	store, err := sqlitetest.Open(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{DataDir: filepath.Join(dir, "data"), RunFilePath: filepath.Join(dir, "running.json")}
	rt := conpty.New(conpty.Options{RunFilePath: cfg.RunFilePath})
	agents, err := buildAgentResolver(config.DefaultAgent, log)
	if err != nil {
		t.Fatal(err)
	}
	lifecycleMessenger := newModeAwareMessenger()
	lcm := newDeliveryLifecycle(store, lifecycleMessenger, agents)
	svc, _, sessMgr, err := startSession(ctx, cfg, rt, store, lcm, newSessionMessenger(store, rt, log),
		telemetryadapter.NoopSink{}, nil, agents, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, log)
	if err != nil {
		t.Fatal(err)
	}
	lifecycleMessenger.Bind(sessionLifecycleMessenger{sessMgr})
	lcm.SetSessionInputLease(sessMgr)
	lcm.SetSessionOperationGate(sessMgr)

	if err := store.UpsertProject(ctx, domain.ProjectRecord{ID: "p", Path: dir, RegisteredAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	rec, err := store.CreateSession(ctx, domain.SessionRecord{
		ProjectID:        "p",
		Kind:             domain.KindWorker,
		Harness:          domain.HarnessCodex,
		Mode:             domain.SessionModeTUI,
		Activity:         domain.Activity{State: domain.ActivityIdle, LastActivityAt: time.Now().UTC()},
		FirstSignalAt:    time.Now().UTC(),
		AutoInjectCI:     true,
		AutoInjectReview: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	f := &nativeDeliveryFixture{dir: dir, store: store, runtime: rt, lcm: lcm, sessMgr: sessMgr, agents: agents, sessions: svc, id: rec.ID}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = rt.Destroy(ctx, ports.RuntimeHandle{ID: string(f.id)})
	})
	return f
}

func newDeliveryLifecycle(store *sqlite.Store, messenger ports.AgentMessenger, agents ports.AgentResolver) *lifecycle.Manager {
	return lifecycle.New(store, messenger,
		lifecycle.WithActiveSteering(activeTurnSteering(agents)),
		lifecycle.WithStartupSignalGate(startupSignalGatesInput(agents)),
		lifecycle.WithUrgentNudgeGate(urgentNudgeWaitingInputSafe(agents)),
	)
}

func (f *nativeDeliveryFixture) inputLog(launch string) string {
	return filepath.Join(f.dir, launch+".input")
}

// launch starts a real native host for this session and commits that launch
// as the current runtime generation, as a spawn or restore would.
func (f *nativeDeliveryFixture) launch(t *testing.T, launch string) {
	t.Helper()
	ctx := context.Background()
	ready := filepath.Join(f.dir, launch+".ready")
	handle, err := f.runtime.Create(ctx, ports.RuntimeConfig{
		SessionID:     f.id,
		WorkspacePath: f.dir,
		Argv:          []string{"/bin/sh", "-c", nativeTestAgent},
		Env: map[string]string{
			"AO_RUNTIME_LAUNCH_ID": launch,
			"AO_TEST_AGENT_READY":  ready,
			"AO_TEST_AGENT_INPUT":  f.inputLog(launch),
		},
	})
	if err != nil {
		t.Fatalf("create native runtime: %v", err)
	}
	waitForFile(t, ready, "ready")
	rec := f.session(t)
	rec.Metadata.RuntimeHandleID = handle.ID
	rec.Metadata.RuntimeLaunchID = launch
	rec.Activity = domain.Activity{State: domain.ActivityIdle, LastActivityAt: time.Now().UTC()}
	if err := f.store.UpdateSession(ctx, rec); err != nil {
		t.Fatal(err)
	}
}

// exitAgent makes the agent exit on its own, the way it does after reporting
// done, without telling AO. Only the PTY child exits; the host stays alive.
func (f *nativeDeliveryFixture) exitAgent(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	handle := ports.RuntimeHandle{ID: string(f.id)}
	if err := f.runtime.SendInput(ctx, handle, "__exit__\r"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		alive, err := f.runtime.IsChildAlive(ctx, handle)
		if err == nil && !alive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("native agent did not exit: alive=%v err=%v", alive, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	hostAlive, err := f.runtime.IsAlive(ctx, handle)
	if err != nil || !hostAlive {
		t.Fatalf("native host must outlive its agent: alive=%v err=%v", hostAlive, err)
	}
	if rec := f.session(t); rec.Activity.State != domain.ActivityIdle || rec.IsTerminated {
		t.Fatalf("precondition: durable row must still be live and idle, got %+v terminated=%v", rec.Activity, rec.IsTerminated)
	}
}

func (f *nativeDeliveryFixture) session(t *testing.T) domain.SessionRecord {
	t.Helper()
	rec, ok, err := f.store.GetSession(context.Background(), f.id)
	if err != nil || !ok {
		t.Fatalf("get session: ok=%v err=%v", ok, err)
	}
	return rec
}

func waitForFile(t *testing.T, path, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
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

func readFile(path string) string {
	raw, _ := os.ReadFile(path)
	return string(raw)
}

// T01: a send to a current TUI launch whose agent has exited, while the row
// still says idle, must not be reported as delivered.
func TestNativeSendToExitedAgentReturnsAgentExited(t *testing.T) {
	f := newNativeDeliveryFixture(t)
	ctx := context.Background()
	f.launch(t, "launch-a")

	// R01: a live agent still receives an ordinary send through the same path.
	if err := f.sessions.Send(ctx, f.id, "R01 live message", nil); err != nil {
		t.Fatalf("live send: %v", err)
	}
	waitForFile(t, f.inputLog("launch-a"), "R01 live message")

	f.exitAgent(t)
	err := f.sessions.Send(ctx, f.id, "T01 message after exit", nil)
	received := readFile(f.inputLog("launch-a"))
	if strings.Contains(received, "T01 message after exit") {
		t.Fatalf("an exited agent cannot read input; agent log=%q", received)
	}
	if err == nil {
		t.Fatalf("T01: send to an exited agent returned success (HTTP 200 ok=true) although the agent never read it; agent log=%q", received)
	}
	rr := httptest.NewRecorder()
	envelope.WriteError(rr, httptest.NewRequest(http.MethodPost, "/api/v1/sessions/"+string(f.id)+"/send", nil), err)
	if rr.Code != 409 || !strings.Contains(rr.Body.String(), "AGENT_EXITED") {
		t.Fatalf("T01: response = %d %s, want 409 AGENT_EXITED (err=%v)", rr.Code, rr.Body.String(), err)
	}
	// A definite exit is not session death: the session and its workspace stay.
	if rec := f.session(t); rec.IsTerminated {
		t.Fatalf("send rejection terminated the session: %+v", rec)
	}
	if _, statErr := os.Stat(f.dir); statErr != nil {
		t.Fatalf("workspace removed: %v", statErr)
	}
}

const t03PRURL = "https://github.com/o/r/pull/7"

func (f *nativeDeliveryFixture) writeFailingPRWithReviewComment(t *testing.T) ports.SCMObservation {
	t.Helper()
	ctx := context.Background()
	if err := f.store.WriteSCMObservation(ctx, domain.PullRequest{
		URL: t03PRURL, SessionID: f.id, Number: 7, HeadSHA: "c1", UpdatedAt: time.Now(), AutoInjectCI: true,
	}, nil, nil, nil, []domain.PullRequestComment{{
		ID: "rc-1", Author: "reviewer", File: "main.go", Line: 42, Body: "handle the nil pointer",
		CreatedAt: time.Now().UTC(), AutoInjectReview: true,
	}}, ports.ReviewWriteReplace); err != nil {
		t.Fatalf("persist PR: %v", err)
	}
	return ports.SCMObservation{
		Fetched: true,
		PR:      ports.SCMPRObservation{URL: t03PRURL, Number: 7, HeadSHA: "c1"},
		CI: ports.SCMCIObservation{
			Summary: string(domain.CIFailing), HeadSHA: "c1",
			FailedChecks: []ports.SCMCheckObservation{{Name: "build", Status: string(domain.PRCheckFailed), LogTail: "boom"}},
		},
	}
}

func (f *nativeDeliveryFixture) dedupState(t *testing.T) map[string]string {
	t.Helper()
	raw, err := f.store.GetPRLastNudgeSignature(context.Background(), t03PRURL)
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Seen map[string]string `json:"seen"`
	}
	if raw != "" {
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			t.Fatalf("decode dedup %q: %v", raw, err)
		}
	}
	return payload.Seen
}

const historicalDedup = `{"seen":{"merge-conflict:https://github.com/o/r/pull/7":"conflicting"},"attempts":{"merge-conflict:https://github.com/o/r/pull/7":1}}`

// T03: review and CI feedback dispatched through the daemon wiring to an
// exited agent must not consume successful-delivery dedup state.
// T04: once a new launch is live, the same observation is delivered exactly
// once, including after lifecycle is rebuilt from the same database.
func TestNativeFeedbackToExitedAgentStaysUndelivered(t *testing.T) {
	f := newNativeDeliveryFixture(t)
	ctx := context.Background()
	f.launch(t, "launch-a")
	obs := f.writeFailingPRWithReviewComment(t)
	if err := f.store.UpdatePRLastNudgeSignature(ctx, t03PRURL, historicalDedup); err != nil {
		t.Fatal(err)
	}
	f.exitAgent(t)

	applyErr := f.lcm.ApplySCMObservation(ctx, f.id, obs)
	seen := f.dedupState(t)
	received := readFile(f.inputLog("launch-a"))
	if strings.Contains(received, "boom") || strings.Contains(received, "handle the nil pointer") {
		t.Fatalf("an exited agent cannot read feedback; agent log=%q", received)
	}
	if _, ok := seen["ci:"+t03PRURL]; ok {
		t.Fatalf("T03: undelivered CI feedback consumed dedup state (applyErr=%v): %v", applyErr, seen)
	}
	for key := range seen {
		if strings.HasPrefix(key, "comment:") {
			t.Fatalf("T03: undelivered review feedback consumed dedup state (applyErr=%v): %v", applyErr, seen)
		}
	}
	if applyErr == nil {
		t.Fatal("T03: feedback rejected for an exited agent must surface an error so the observer retries")
	}
	if seen["merge-conflict:"+t03PRURL] != "conflicting" {
		t.Fatalf("unrelated historical dedup changed: %v", seen)
	}
	if rec := f.session(t); rec.IsTerminated {
		t.Fatalf("feedback rejection terminated the session: %+v", rec)
	}

	// T04: recovery to a new launch B. Rebuild lifecycle from the same DB
	// before the first successful delivery: nothing durable may suppress it.
	destroyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	if err := f.runtime.Destroy(destroyCtx, ports.RuntimeHandle{ID: string(f.id)}); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	f.launch(t, "launch-b")
	restarted := newDeliveryLifecycle(f.store, sessionLifecycleMessenger{f.sessMgr}, f.agents)
	restarted.SetSessionInputLease(f.sessMgr)
	restarted.SetSessionOperationGate(f.sessMgr)
	if err := restarted.ApplySCMObservation(ctx, f.id, obs); err != nil {
		t.Fatalf("T04: first delivery after recovery: %v", err)
	}
	waitForFile(t, f.inputLog("launch-b"), "boom")
	waitForFile(t, f.inputLog("launch-b"), "handle the nil pointer")
	seen = f.dedupState(t)
	if _, ok := seen["ci:"+t03PRURL]; !ok {
		t.Fatalf("T04: delivered CI feedback was not recorded: %v", seen)
	}
	if _, ok := seen["comment:"+t03PRURL+":rc-1"]; !ok {
		t.Fatalf("T04: delivered review feedback was not recorded: %v", seen)
	}
	if seen["merge-conflict:"+t03PRURL] != "conflicting" {
		t.Fatalf("unrelated historical dedup changed: %v", seen)
	}
	if err := restarted.ApplySCMObservation(ctx, f.id, obs); err != nil {
		t.Fatalf("T04: duplicate observation: %v", err)
	}
	// A duplicate observation after a second rebuild must stay deduplicated.
	again := newDeliveryLifecycle(f.store, sessionLifecycleMessenger{f.sessMgr}, f.agents)
	again.SetSessionInputLease(f.sessMgr)
	if err := again.ApplySCMObservation(ctx, f.id, obs); err != nil {
		t.Fatalf("T04: duplicate observation after rebuild: %v", err)
	}
	if err := f.sessions.Send(ctx, f.id, "T04 fence", nil); err != nil {
		t.Fatalf("fence send: %v", err)
	}
	waitForFile(t, f.inputLog("launch-b"), "T04 fence")
	log := readFile(f.inputLog("launch-b"))
	if n := strings.Count(log, "handle the nil pointer"); n != 1 {
		t.Fatalf("T04: review feedback delivered %d times, want 1; log=%q", n, log)
	}
	if n := strings.Count(log, "boom"); n != 1 {
		t.Fatalf("T04: CI feedback delivered %d times, want 1; log=%q", n, log)
	}
}

// T06: after recovery to launch B, launch A's delayed supervisor exit report
// and delayed reaper observation cannot fence B, and a send reaches only B.
func TestNativeStaleLaunchFactsCannotFenceNewLaunch(t *testing.T) {
	f := newNativeDeliveryFixture(t)
	ctx := context.Background()
	f.launch(t, "launch-a")
	f.exitAgent(t)
	destroyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	if err := f.runtime.Destroy(destroyCtx, ports.RuntimeHandle{ID: string(f.id)}); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	f.launch(t, "launch-b")

	if err := f.lcm.ApplyActivitySignal(ctx, f.id, ports.ActivitySignal{
		Valid: true, State: domain.ActivityExited, Event: "process-exited", LaunchID: "launch-a",
	}); err != nil {
		t.Fatalf("delayed exit report: %v", err)
	}
	if err := f.lcm.ApplyRuntimeObservation(ctx, f.id, ports.RuntimeFacts{
		ObservedAt: time.Now().UTC(), Runtime: ports.ProbeAlive, Workload: ports.ProbeDead, LaunchID: "launch-a",
	}); err != nil {
		t.Fatalf("delayed reaper observation: %v", err)
	}
	if rec := f.session(t); rec.Activity.State != domain.ActivityIdle || rec.IsTerminated || rec.Metadata.RuntimeLaunchID != "launch-b" {
		t.Fatalf("launch A's facts changed launch B: %+v launch=%s", rec.Activity, rec.Metadata.RuntimeLaunchID)
	}
	if err := f.sessions.Send(ctx, f.id, "T06 message for B", nil); err != nil {
		t.Fatalf("send to launch B: %v", err)
	}
	waitForFile(t, f.inputLog("launch-b"), "T06 message for B")
	if strings.Contains(readFile(f.inputLog("launch-a")), "T06 message for B") {
		t.Fatal("launch B's message reached launch A")
	}
}
