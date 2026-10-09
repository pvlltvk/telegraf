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
	stop = make(chan struct{})
	processStop := stop
	done := make(chan error, 1)
	go func() { done <- agent.reloadLoop() }()
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

func TestReloadRejectsStaticErrorsAndAcceptsCorrection(t *testing.T) {
	counters := reloadPlugins(t)
	path := filepath.Join(t.TempDir(), "telegraf.conf")
	require.NoError(t, os.WriteFile(path, []byte(reloadConfig("original")), 0600))
	agent := &Telegraf{GlobalFlags: GlobalFlags{config: []string{path}, watchConfig: "poll", watchInterval: 5 * time.Millisecond, quiet: true}}
	startReloadLoop(t, agent)
	require.Eventually(t, func() bool { return counters.gathers.Load() >= 2 }, time.Second, 5*time.Millisecond)
	for _, data := range []string{
		"[[inputs.not_in_binary]]\n[[outputs.reload_test]]",
		"[[inputs.reload_test]",
		reloadConfig("invalid") + "unknown_option = true\n",
		reloadConfig("invalid") + "flush_interval = -1\n",
		"[[inputs.reload_test]]",
		"[agent]\ninterval = '-1s'\n[[inputs.reload_test]]\n[[outputs.reload_test]]",
	} {
		before := counters.gathers.Load()
		require.NoError(t, os.WriteFile(path, []byte(data), 0600))
		require.Eventually(t, func() bool { return counters.gathers.Load() >= before+5 }, time.Second, 5*time.Millisecond)
		require.EqualValues(t, 1, counters.starts.Load())
		require.Zero(t, counters.stops.Load())
	}
	require.NoError(t, os.WriteFile(path, []byte(reloadConfig("corrected")), 0600))
	require.Eventually(t, func() bool {
		for {
			select {
			case name := <-counters.names:
				if name == "corrected" {
					return true
				}
			default:
				return false
			}
		}
	}, 2*time.Second, 5*time.Millisecond)
}

func TestReloadInitializationFailureRemainsFatal(t *testing.T) {
	counters := reloadPlugins(t)
	path := filepath.Join(t.TempDir(), "telegraf.conf")
	require.NoError(t, os.WriteFile(path, []byte(reloadConfig("original")), 0600))
	agent := &Telegraf{GlobalFlags: GlobalFlags{config: []string{path}, watchConfig: "poll", watchInterval: 5 * time.Millisecond, quiet: true}}
	done := startReloadLoop(t, agent)
	require.Eventually(t, func() bool { return counters.gathers.Load() >= 2 }, time.Second, 5*time.Millisecond)
	replacement := "[agent]\ninterval='10ms'\n[[inputs.reload_test]]\nfail_init=true\n[[outputs.reload_test]]"
	require.NoError(t, os.WriteFile(path, []byte(replacement), 0600))
	select {
	case err := <-done:
		require.ErrorContains(t, err, "replacement initialization failed")
		done <- err
	case <-time.After(2 * time.Second):
		t.Fatal("initialization failure did not stop agent")
	}
	require.EqualValues(t, 1, counters.starts.Load())
	require.EqualValues(t, 1, counters.stops.Load())
}

func TestRemoteWatcherRejectsHTTPErrorAndPreservesBaseline(t *testing.T) {
	var status atomic.Int64
	status.Store(http.StatusServiceUnavailable)
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Last-Modified", "new")
		w.WriteHeader(int(status.Load()))
	}))
	defer server.Close()
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

func TestRemoteWatcherMissingLastModifiedDisablesSource(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); w.WriteHeader(http.StatusOK) }))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	agent := &Telegraf{}
	agent.watchRemoteConfigs(ctx, make(chan remoteRevision, 1), 5*time.Millisecond, map[string]string{server.URL: "accepted"})
	require.EqualValues(t, 1, calls.Load())
	agent.watchRemoteConfigs(ctx, make(chan remoteRevision, 1), time.Second, map[string]string{server.URL: ""})
	require.EqualValues(t, 1, calls.Load())
}

func TestRemoteWatcherOneChangeOneReload(t *testing.T) {
	var modified atomic.Value
	modified.Store("v1")
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Last-Modified", modified.Load().(string))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changes := make(chan remoteRevision, 4)
	done := make(chan struct{})
	agent := &Telegraf{}
	go func() {
		agent.watchRemoteConfigs(ctx, changes, 5*time.Millisecond, map[string]string{server.URL: "v1"})
		close(done)
	}()

	require.Eventually(t, func() bool { return calls.Load() >= 3 }, time.Second, 5*time.Millisecond)
	require.Empty(t, changes)

	modified.Store("v2")
	select {
	case revision := <-changes:
		require.Equal(t, remoteRevision{path: server.URL, modified: "v2"}, revision)
	case <-time.After(time.Second):
		t.Fatal("first change did not trigger reload")
	}
	before := calls.Load()
	require.Eventually(t, func() bool { return calls.Load() >= before+3 }, time.Second, 5*time.Millisecond)
	require.Empty(t, changes, "first change must not trigger more than one reload")

	modified.Store("v3")
	select {
	case revision := <-changes:
		require.Equal(t, remoteRevision{path: server.URL, modified: "v3"}, revision)
	case <-time.After(time.Second):
		t.Fatal("second change did not trigger reload")
	}
	before = calls.Load()
	require.Eventually(t, func() bool { return calls.Load() >= before+3 }, time.Second, 5*time.Millisecond)
	require.Empty(t, changes, "second change must not trigger more than one reload")

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

	// Serve invalid config at a new Last-Modified. The watcher detects
	// the change, staging fails, and the reloadLoop restarts the watcher
	// with the old baseline so it retries on the next interval.
	body.Store("invalid TOML {{{\n")
	lastMod.Store("v2")
	before := counters.gathers.Load()
	require.Eventually(t, func() bool { return counters.gathers.Load() >= before+5 }, time.Second, 5*time.Millisecond)
	require.EqualValues(t, 1, counters.starts.Load())
	require.Zero(t, counters.stops.Load())

	// Fix the remote config without changing Last-Modified. The retried
	// watcher detects the same mismatch and the valid content activates.
	body.Store(reloadConfig("corrected"))
	require.Eventually(t, func() bool {
		for {
			select {
			case name := <-counters.names:
				if name == "corrected" {
					return true
				}
			default:
				return false
			}
		}
	}, 2*time.Second, 5*time.Millisecond)
	require.EqualValues(t, 2, counters.starts.Load())
	require.EqualValues(t, 1, counters.stops.Load())

	before = counters.gathers.Load()
	require.Eventually(t, func() bool { return counters.gathers.Load() >= before+5 }, time.Second, 5*time.Millisecond)
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
			// Confirm subscription before replacing the watched inode.
			require.Eventually(t, func() bool {
				if err := os.WriteFile(path, []byte(strconv.FormatInt(time.Now().UnixNano(), 10)), 0600); err != nil {
					return false
				}
				select {
				case <-reload:
					return true
				default:
					return false
				}
			}, time.Second, 10*time.Millisecond)
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
		require.Eventually(t, func() bool {
			if err := os.WriteFile(path, []byte(strconv.FormatInt(time.Now().UnixNano(), 10)), 0600); err != nil {
				return false
			}
			select {
			case <-reload:
				return true
			default:
				return false
			}
		}, time.Second, 10*time.Millisecond)
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
		require.Eventually(t, func() bool {
			for {
				select {
				case gathered := <-counters.names:
					if gathered == name {
						return true
					}
				default:
					return false
				}
			}
		}, 2*time.Second, 5*time.Millisecond, "edit %d was not activated", i)
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
			// Hold the second changed HEAD of the pass until staging has fetched both sources.
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
