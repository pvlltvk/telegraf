package config

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/parsers"
)

type stagingParserFuncInput struct {
	stagingInput
	parserFunc telegraf.ParserFunc
}

func (i *stagingParserFuncInput) SetParserFunc(fn telegraf.ParserFunc) { i.parserFunc = fn }

func TestStageRejectsUnavailableSelectedParser(t *testing.T) {
	registerStagingPlugins(t)
	inputs.Inputs["staging_parser_func"] = func() telegraf.Input { return &stagingParserFuncInput{} }
	t.Cleanup(func() { delete(inputs.Inputs, "staging_parser_func") })
	creator, registered := parsers.Parsers["influx_upstream"]
	delete(parsers.Parsers, "influx_upstream")
	t.Cleanup(func() {
		if registered {
			parsers.Parsers["influx_upstream"] = creator
		}
	})
	path := stagingFile(t, `[[inputs.staging_parser_func]]
 data_format = "influx"
 influx_parser_type = "upstream"
 [[outputs.reload_staging]]
`)
	_, err := NewStagingConfig().Stage(context.Background(), time.Second, path)
	require.Error(t, err)
}

func TestSnapshotDoesNotRefetchRemoteSource(t *testing.T) {
	registerStagingPlugins(t)
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) != 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Last-Modified", "accepted-version")
		_, _ = fmt.Fprint(w, "[[inputs.reload_staging]]\nname='validated'\n[[outputs.reload_staging]]")
	}))
	defer server.Close()
	snapshot, err := NewStagingConfig().Stage(context.Background(), time.Second, server.URL)
	require.NoError(t, err)
	loaded := NewConfig()
	require.NoError(t, snapshot.Load(loaded))
	defer loaded.Outputs[0].Close()
	require.EqualValues(t, 1, requests.Load())
	require.Equal(t, "accepted-version", snapshot.LastModified[server.URL])
	require.Equal(t, "validated", loaded.Inputs[0].Input.(*stagingInput).Name)
}
