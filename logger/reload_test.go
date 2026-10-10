package logger

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
)

type reloadSink struct {
	closed            bool
	writtenAfterClose bool
}

func (s *reloadSink) Print(telegraf.LogLevel, time.Time, string, map[string]any, ...any) {
	if s.closed {
		s.writtenAfterClose = true
	}
}
func (s *reloadSink) Close() error { s.closed = true; return nil }

func TestLoggingReconfigurationWhileWriting(t *testing.T) {
	previous := instance
	instance = defaultHandler()
	t.Cleanup(func() { instance = previous })
	logger := New("agent", "reload", "")
	var workers sync.WaitGroup
	workers.Go(func() {
		for range 1000 {
			logger.Info("Collecting")
			_ = logger.Level()
		}
	})
	sinks := make([]*reloadSink, 100)
	for i := range sinks {
		sinks[i] = &reloadSink{}
		require.NoError(t, instance.switchSink(sinks[i], telegraf.Info, time.UTC, true))
	}
	workers.Wait()
	require.NoError(t, instance.close())
	for _, sink := range sinks {
		require.True(t, sink.closed)
		require.False(t, sink.writtenAfterClose)
	}
}
