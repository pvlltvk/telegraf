package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/logger"
)

func TestStageRejectsInvalidLoggingSettings(t *testing.T) {
	reloadPlugins(t)
	for _, settings := range []string{
		"logformat='unavailable'",
		"log_with_timezone='Unavailable/Timezone'",
		"logtarget='unavailable'",
		"logtarget='eventlog'\nlogformat='text'",
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

func TestReloadRejectsLoggingErrorWhileCollectionContinues(t *testing.T) {
	counters := reloadPlugins(t)
	path := filepath.Join(t.TempDir(), "telegraf.conf")
	require.NoError(t, os.WriteFile(path, []byte(reloadConfig("original")), 0600))
	rejected := make(chan struct{}, 1)
	callback, err := logger.AddCallback(func(_ telegraf.LogLevel, _ time.Time, _ string, _ map[string]any, arguments ...any) {
		if strings.Contains(fmt.Sprint(arguments...), "Config reload rejected") {
			select {
			case rejected <- struct{}{}:
			default:
			}
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() { logger.RemoveCallback(callback) })
	agent := &Telegraf{GlobalFlags: GlobalFlags{config: []string{path}, watchConfig: "poll", watchInterval: 5 * time.Millisecond, quiet: true}}
	done := startReloadLoop(t, agent)
	require.Eventually(t, func() bool { return counters.gathers.Load() >= 2 }, time.Second, 5*time.Millisecond)
	replacement := strings.Replace(reloadConfig("invalid"), "[agent]", "[agent]\nlogformat='unavailable'", 1)
	require.NoError(t, os.WriteFile(path, []byte(replacement), 0600))
	select {
	case <-rejected:
	case err := <-done:
		done <- err
		t.Fatalf("invalid logging settings terminated collection: %v", err)
	case <-time.After(time.Second):
		t.Fatal("invalid logging settings did not reject reload")
	}
	before := counters.gathers.Load()
	require.Eventually(t, func() bool { return counters.gathers.Load() >= before+5 }, time.Second, 5*time.Millisecond)
	require.EqualValues(t, 1, counters.starts.Load())
	require.Zero(t, counters.stops.Load())
	require.NoError(t, os.WriteFile(path, []byte(reloadConfig("corrected")), 0600))
	require.Eventually(t, func() bool { return counters.starts.Load() >= 2 }, time.Second, 5*time.Millisecond)
}

func TestReloadServiceInputStartupFailureRemainsFatal(t *testing.T) {
	counters := reloadPlugins(t)
	path := filepath.Join(t.TempDir(), "telegraf.conf")
	require.NoError(t, os.WriteFile(path, []byte(reloadConfig("original")), 0600))
	agent := &Telegraf{GlobalFlags: GlobalFlags{config: []string{path}, watchConfig: "poll", watchInterval: 5 * time.Millisecond, quiet: true}}
	done := startReloadLoop(t, agent)
	require.Eventually(t, func() bool { return counters.gathers.Load() >= 2 }, time.Second, 5*time.Millisecond)
	replacement := "[agent]\ninterval='10ms'\n[[inputs.reload_test]]\nfail_start=true\n[[outputs.reload_test]]"
	require.NoError(t, os.WriteFile(path, []byte(replacement), 0600))
	select {
	case err := <-done:
		require.ErrorContains(t, err, "replacement startup failed")
		done <- err
	case <-time.After(2 * time.Second):
		t.Fatal("service input startup failure did not stop agent")
	}
	require.EqualValues(t, 1, counters.starts.Load())
	require.EqualValues(t, 1, counters.stops.Load())
}
