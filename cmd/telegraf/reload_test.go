package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/outputs"
)

type reloadCounters struct {
	starts  atomic.Int64
	stops   atomic.Int64
	gathers atomic.Int64
	names   chan string
}

type reloadInput struct {
	Name      string
	FailInit  bool
	FailStart bool
	counters  *reloadCounters
}

func (*reloadInput) SampleConfig() string { return "" }
func (i *reloadInput) Init() error {
	if i.FailInit {
		return errors.New("replacement initialization failed")
	}
	return nil
}
func (i *reloadInput) Start(telegraf.Accumulator) error {
	if i.FailStart {
		return errors.New("replacement startup failed")
	}
	i.counters.starts.Add(1)
	return nil
}
func (i *reloadInput) Stop() { i.counters.stops.Add(1) }
func (i *reloadInput) Gather(acc telegraf.Accumulator) error {
	i.counters.gathers.Add(1)
	select {
	case i.counters.names <- i.Name:
	default:
	}
	acc.AddGauge("reload", map[string]any{"value": 1}, nil)
	return nil
}

type reloadOutput struct{}

func (*reloadOutput) SampleConfig() string          { return "" }
func (*reloadOutput) Connect() error                { return nil }
func (*reloadOutput) Close() error                  { return nil }
func (*reloadOutput) Write([]telegraf.Metric) error { return nil }

func reloadPlugins(t *testing.T) *reloadCounters {
	t.Helper()
	counters := &reloadCounters{names: make(chan string, 128)}
	inputs.Inputs["reload_test"] = func() telegraf.Input { return &reloadInput{counters: counters} }
	outputs.Outputs["reload_test"] = func() telegraf.Output { return &reloadOutput{} }
	t.Cleanup(func() { delete(inputs.Inputs, "reload_test"); delete(outputs.Outputs, "reload_test") })
	return counters
}

func reloadConfig(name string) string {
	return fmt.Sprintf(`[agent]
 interval = "10ms"
 flush_interval = "10ms"
 round_interval = false
 [[inputs.reload_test]]
 name = %q
 [[outputs.reload_test]]
`, name)
}

func startReloadLoop(t *testing.T, agent *Telegraf) chan error {
	t.Helper()
	staged := agent.stageConfiguration(context.Background())
	require.NoError(t, staged.err)
	require.NoError(t, agent.activateConfiguration(staged))
	stop = make(chan struct{})
	processStop := stop
	done := make(chan error, 1)
	go func() { done <- agent.reloadLoop(staged.snapshot.LastModified) }()
	t.Cleanup(func() {
		select {
		case <-processStop:
		default:
			close(processStop)
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("agent did not stop")
		}
	})
	return done
}

func waitForName(t *testing.T, counters *reloadCounters, name string) {
	t.Helper()
	require.Eventually(t, func() bool {
		for {
			select {
			case n := <-counters.names:
				if n == name {
					return true
				}
			default:
				return false
			}
		}
	}, 2*time.Second, 5*time.Millisecond)
}

func waitReload(t *testing.T, reload <-chan struct{}, path, content string) {
	t.Helper()
	require.Eventually(t, func() bool {
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			return false
		}
		select {
		case <-reload:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestInitialLoadFailureReturnsError(t *testing.T) {
	reloadPlugins(t)
	path := filepath.Join(t.TempDir(), "telegraf.conf")
	require.NoError(t, os.WriteFile(path, []byte("[[inputs.not_in_binary]]"), 0600))
	agent := &Telegraf{GlobalFlags: GlobalFlags{config: []string{path}}}
	staged := agent.stageConfiguration(context.Background())
	require.Error(t, staged.err)
}

func TestReloadRejectsStaticErrorsAndAcceptsCorrection(t *testing.T) {
	counters := reloadPlugins(t)
	path := filepath.Join(t.TempDir(), "telegraf.conf")
	require.NoError(t, os.WriteFile(path, []byte(reloadConfig("original")), 0600))
	agent := &Telegraf{GlobalFlags: GlobalFlags{config: []string{path}, watchConfig: "poll", watchInterval: 5 * time.Millisecond, quiet: true}}
	startReloadLoop(t, agent)
	require.Eventually(t, func() bool { return counters.gathers.Load() >= 2 }, time.Second, 5*time.Millisecond)
	for _, data := range []string{
		"[[inputs.reload_test]",
		"[[inputs.not_in_binary]]\n[[outputs.reload_test]]",
		reloadConfig("invalid") + "unknown_option = true\n",
		strings.Replace(reloadConfig("invalid"), "[agent]", "[agent]\nlogformat='unavailable'", 1),
	} {
		before := counters.gathers.Load()
		require.NoError(t, os.WriteFile(path, []byte(data), 0600))
		require.Eventually(t, func() bool { return counters.gathers.Load() >= before+5 }, time.Second, 5*time.Millisecond)
		require.EqualValues(t, 1, counters.starts.Load())
		require.Zero(t, counters.stops.Load())
	}
	require.NoError(t, os.WriteFile(path, []byte(reloadConfig("corrected")), 0600))
	waitForName(t, counters, "corrected")
}

func TestReloadActivationFailureRemainsFatal(t *testing.T) {
	for _, tc := range []struct {
		name, replacement, errMsg string
	}{
		{"init", "[agent]\ninterval='10ms'\n[[inputs.reload_test]]\nfail_init=true\n[[outputs.reload_test]]", "replacement initialization failed"},
		{"start", "[agent]\ninterval='10ms'\n[[inputs.reload_test]]\nfail_start=true\n[[outputs.reload_test]]", "replacement startup failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counters := reloadPlugins(t)
			path := filepath.Join(t.TempDir(), "telegraf.conf")
			require.NoError(t, os.WriteFile(path, []byte(reloadConfig("original")), 0600))
			agent := &Telegraf{GlobalFlags: GlobalFlags{config: []string{path}, watchConfig: "poll", watchInterval: 5 * time.Millisecond, quiet: true}}
			done := startReloadLoop(t, agent)
			require.Eventually(t, func() bool { return counters.gathers.Load() >= 2 }, time.Second, 5*time.Millisecond)
			require.NoError(t, os.WriteFile(path, []byte(tc.replacement), 0600))
			select {
			case err := <-done:
				require.ErrorContains(t, err, tc.errMsg)
				done <- err
			case <-time.After(2 * time.Second):
				t.Fatal("activation failure did not stop agent")
			}
			require.EqualValues(t, 1, counters.starts.Load())
			require.EqualValues(t, 1, counters.stops.Load())
		})
	}
}

func TestStageRejectsInvalidLoggingSettings(t *testing.T) {
	reloadPlugins(t)
	for _, settings := range []string{
		"logformat='unavailable'",
		"log_with_timezone='Unavailable/Timezone'",
		"logtarget='unavailable'",
	} {
		t.Run(settings, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "telegraf.conf")
			data := strings.Replace(reloadConfig("candidate"), "[agent]", "[agent]\n"+settings, 1)
			require.NoError(t, os.WriteFile(path, []byte(data), 0600))
			agent := &Telegraf{GlobalFlags: GlobalFlags{config: []string{path}, quiet: true}}
			require.Error(t, agent.stageConfiguration(context.Background()).err)
		})
	}
}

func TestRemoteWatcherRejectsHTTPErrorAndAcceptsChange(t *testing.T) {
	var status atomic.Int64
	status.Store(http.StatusServiceUnavailable)
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Last-Modified", "new")
		w.WriteHeader(int(status.Load()))
	}))
	defer server.Close()

	t.Run("missing Last-Modified disables source", func(t *testing.T) {
		var noLMCalls atomic.Int64
		noLM := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			noLMCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		defer noLM.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		agent := &Telegraf{}
		agent.watchRemoteConfigs(ctx, make(chan remoteRevision, 1), 5*time.Millisecond, map[string]string{noLM.URL: "accepted"})
		require.EqualValues(t, 1, noLMCalls.Load())
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changes := make(chan remoteRevision, 1)
	done := make(chan struct{})
	agent := &Telegraf{}
	go func() {
		agent.watchRemoteConfigs(ctx, changes, 5*time.Millisecond, map[string]string{server.URL: "accepted"})
		close(done)
	}()
	require.Eventually(t, func() bool { return calls.Load() >= 3 }, time.Second, 5*time.Millisecond)
	require.Empty(t, changes)
	status.Store(http.StatusOK)
	select {
	case <-changes:
	case <-time.After(time.Second):
		t.Fatal("changed config did not trigger reload")
	}
	before := calls.Load()
	require.Eventually(t, func() bool { return calls.Load() >= before+3 }, time.Second, 5*time.Millisecond)
	require.Empty(t, changes, "same Last-Modified value must not trigger a second reload")
	cancel()
	<-done
}

func TestRemoteReloadRetriesAfterRejectionAndActivatesOnce(t *testing.T) {
	counters := reloadPlugins(t)
	var lastMod atomic.Value
	lastMod.Store("v1")
	var body atomic.Value
	body.Store(reloadConfig("original"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", lastMod.Load().(string))
		if r.Method == http.MethodHead {
			return
		}
		_, _ = fmt.Fprint(w, body.Load().(string))
	}))
	defer server.Close()
	agent := &Telegraf{GlobalFlags: GlobalFlags{
		config:                 []string{server.URL},
		configURLWatchInterval: 5 * time.Millisecond,
		quiet:                  true,
	}}
	startReloadLoop(t, agent)
	require.Eventually(t, func() bool { return counters.gathers.Load() >= 2 }, time.Second, 5*time.Millisecond)
	body.Store("invalid TOML {{{\n")
	lastMod.Store("v2")
	before := counters.gathers.Load()
	require.Eventually(t, func() bool { return counters.gathers.Load() >= before+5 }, time.Second, 5*time.Millisecond)
	require.EqualValues(t, 1, counters.starts.Load())
	require.Zero(t, counters.stops.Load())
	body.Store(reloadConfig("corrected"))
	waitForName(t, counters, "corrected")
	require.EqualValues(t, 2, counters.starts.Load())
	require.EqualValues(t, 1, counters.stops.Load())
}

func TestReloadShutdownCancelsStagingWhileCollectionContinues(t *testing.T) {
	counters := reloadPlugins(t)
	var stalled atomic.Bool
	started := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if stalled.Load() {
			w.Header().Set("Last-Modified", "replacement")
			if r.Method == http.MethodHead {
				return
			}
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			select {
			case started <- struct{}{}:
			default:
			}
			<-r.Context().Done()
			return
		}
		w.Header().Set("Last-Modified", "original")
		if r.Method != http.MethodHead {
			_, _ = fmt.Fprint(w, reloadConfig("original"))
		}
	}))
	defer server.Close()
	agent := &Telegraf{GlobalFlags: GlobalFlags{config: []string{server.URL},
		configURLWatchInterval: 5 * time.Millisecond,
		configURLTimeout:       time.Minute, quiet: true}}
	done := startReloadLoop(t, agent)
	require.Eventually(t, func() bool { return counters.gathers.Load() >= 2 }, time.Second, 5*time.Millisecond)
	stalled.Store(true)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("replacement fetch did not start")
	}
	before := counters.gathers.Load()
	require.Eventually(t, func() bool { return counters.gathers.Load() >= before+5 }, time.Second, 5*time.Millisecond)
	require.EqualValues(t, 1, counters.starts.Load())
	require.Zero(t, counters.stops.Load())
	close(stop)
	select {
	case err := <-done:
		require.NoError(t, err)
		done <- err
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel config acquisition")
	}
	require.EqualValues(t, 1, counters.stops.Load())
}

func TestLocalWatcherRearmsAfterReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "telegraf.conf")
	require.NoError(t, os.WriteFile(path, []byte("original"), 0600))
	for _, mode := range []string{"poll", "notify"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reload := make(chan struct{}, 1)
			done := make(chan struct{})
			agent := &Telegraf{GlobalFlags: GlobalFlags{watchConfig: mode, watchInterval: 5 * time.Millisecond}}
			go func() { agent.watchLocalConfig(ctx, reload, path); close(done) }()
			waitReload(t, reload, path, strconv.FormatInt(time.Now().UnixNano(), 10))
			require.NoError(t, os.Remove(path))
			select {
			case <-reload:
			case <-time.After(2 * time.Second):
				t.Fatal("deletion did not trigger reload")
			}
			require.NoError(t, os.WriteFile(path, []byte("recreated"), 0600))
			select {
			case <-reload:
			case <-time.After(2 * time.Second):
				t.Fatal("creation did not rearm watcher")
			}
			require.NoError(t, os.WriteFile(path, []byte("corrected"), 0600))
			select {
			case <-reload:
			case <-time.After(time.Second):
				t.Fatal("correction did not trigger reload")
			}
			cancel()
			<-done
		})
	}
}

func TestNotifyWatcherReleasesSubscriptionOnReturn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "telegraf.conf")
	require.NoError(t, os.WriteFile(path, []byte("original"), 0600))
	agent := &Telegraf{GlobalFlags: GlobalFlags{watchConfig: "notify"}}
	for range 5 {
		ctx, cancel := context.WithCancel(context.Background())
		reload := make(chan struct{}, 1)
		done := make(chan struct{})
		go func() { agent.watchLocalConfig(ctx, reload, path); close(done) }()
		waitReload(t, reload, path, strconv.FormatInt(time.Now().UnixNano(), 10))
		cancel()
		<-done
	}
}

func TestReloadNotifyWatcherSurvivesReplacement(t *testing.T) {
	counters := reloadPlugins(t)
	path := filepath.Join(t.TempDir(), "telegraf.conf")
	require.NoError(t, os.WriteFile(path, []byte(reloadConfig("original")), 0600))
	agent := &Telegraf{GlobalFlags: GlobalFlags{config: []string{path}, watchConfig: "notify", quiet: true}}
	startReloadLoop(t, agent)
	require.Eventually(t, func() bool { return counters.gathers.Load() >= 2 }, time.Second, 5*time.Millisecond)
	for i := range 3 {
		name := "replacement" + strconv.Itoa(i)
		require.NoError(t, os.WriteFile(path, []byte(reloadConfig(name)), 0600))
		waitForName(t, counters, name)
	}
}

func TestRemoteReloadCoalescesChangesCoveredBySnapshot(t *testing.T) {
	counters := reloadPlugins(t)
	var revision atomic.Value
	revision.Store("v1")
	var heads, gets atomic.Int64
	staged := make(chan struct{})
	var stagedOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rev := revision.Load().(string)
		w.Header().Set("Last-Modified", rev)
		if r.Method == http.MethodHead {
			if rev == "v2" && heads.Add(1) == 2 {
				<-staged
			}
			return
		}
		if rev == "v2" && gets.Add(1) == 2 {
			stagedOnce.Do(func() { close(staged) })
		}
		if r.URL.Path == "/a" {
			_, _ = fmt.Fprint(w, reloadConfig(rev))
		} else {
			_, _ = fmt.Fprintf(w, "# %s\n[[outputs.reload_test]]\n", rev)
		}
	}))
	defer server.Close()
	defer stagedOnce.Do(func() { close(staged) })

	agent := &Telegraf{GlobalFlags: GlobalFlags{
		config:                 []string{server.URL + "/a", server.URL + "/b"},
		configURLWatchInterval: 5 * time.Millisecond,
		quiet:                  true,
	}}
	startReloadLoop(t, agent)
	require.Eventually(t, func() bool { return counters.gathers.Load() >= 2 }, time.Second, 5*time.Millisecond)
	revision.Store("v2")
	require.Eventually(t, func() bool { return counters.starts.Load() == 2 }, 2*time.Second, 5*time.Millisecond)
	before := counters.gathers.Load()
	require.Eventually(t, func() bool { return counters.gathers.Load() >= before+20 }, 2*time.Second, 5*time.Millisecond)
	require.EqualValues(t, 2, counters.starts.Load())
}
