package config

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/aggregators"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/parsers"
	"github.com/influxdata/telegraf/selfstat"
)

type stagingEmbedded struct{ Embedded Secret }
type stagingNested struct {
	Token Secret
	Name  string
}
type stagingInput struct {
	stagingEmbedded
	Token   Secret
	Pointer *Secret
	Nested  *stagingNested
	Entries []stagingNested
	Tokens  []Secret
	Mapping map[string]Secret
	Ignored Secret `toml:"-"`
	Tagged  Secret `toml:"custom_token"`
	Name    string
	Count   int
}

func (*stagingInput) SampleConfig() string              { return "" }
func (*stagingInput) Gather(telegraf.Accumulator) error { return nil }
func (*stagingInput) Init() error                       { return errors.New("activation only") }

type stagingOutput struct{}

func (*stagingOutput) SampleConfig() string          { return "" }
func (*stagingOutput) Connect() error                { return nil }
func (*stagingOutput) Close() error                  { return nil }
func (*stagingOutput) Write([]telegraf.Metric) error { return nil }

type stagingSerializerFuncOutput struct {
	stagingOutput
	serializerFunc telegraf.SerializerFunc
}

func (o *stagingSerializerFuncOutput) SetSerializerFunc(fn telegraf.SerializerFunc) {
	o.serializerFunc = fn
}

type stagingAggregator struct{}

func (*stagingAggregator) SampleConfig() string      { return "" }
func (*stagingAggregator) Add(telegraf.Metric)       {}
func (*stagingAggregator) Push(telegraf.Accumulator) {}
func (*stagingAggregator) Reset()                    {}

type stagingParserFuncInput struct {
	stagingInput
	parserFunc telegraf.ParserFunc
}

func (i *stagingParserFuncInput) SetParserFunc(fn telegraf.ParserFunc) { i.parserFunc = fn }

func registerStagingPlugins(t *testing.T) {
	t.Helper()
	inputs.Inputs["reload_staging"] = func() telegraf.Input { return &stagingInput{} }
	inputs.Inputs["staging_parser_func"] = func() telegraf.Input { return &stagingParserFuncInput{} }
	outputs.Outputs["reload_staging"] = func() telegraf.Output { return &stagingOutput{} }
	outputs.Outputs["reload_staging_serializer"] = func() telegraf.Output { return &stagingSerializerFuncOutput{} }
	aggregators.Aggregators["reload_staging"] = func() telegraf.Aggregator { return &stagingAggregator{} }
	t.Cleanup(func() {
		delete(inputs.Inputs, "reload_staging")
		delete(inputs.Inputs, "staging_parser_func")
		delete(outputs.Outputs, "reload_staging")
		delete(outputs.Outputs, "reload_staging_serializer")
		delete(aggregators.Aggregators, "reload_staging")
	})
}

func stagingFile(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "telegraf.conf")
	require.NoError(t, os.WriteFile(path, []byte(data), 0600))
	return path
}

func TestStageIsolatesSecretsAndSources(t *testing.T) {
	registerStagingPlugins(t)
	active := NewConfig()
	require.NoError(t, active.LoadConfigData([]byte(`[[inputs.reload_staging]]
 token = "@{active:key}"
`), "active.conf"))
	activeSecret := &active.Inputs[0].Input.(*stagingInput).Token
	defer activeSecret.Destroy()
	publishSource("active.conf")
	count := secretCount.Load()
	unlinked := append(make([]*Secret, 0, len(unlinkedSecrets)), unlinkedSecrets...)
	path := stagingFile(t, `[[inputs.reload_staging]]
 token = "@{replacement:key}"
 pointer = "pointer"
 embedded = "embedded"
 custom_token = "tagged"
 tokens = ["first", "second"]
 name = "candidate"
 [inputs.reload_staging.nested]
 token = "nested"
 name = "nested name"
 [[inputs.reload_staging.entries]]
 token = "entry"
 name = "entry name"
 [inputs.reload_staging.mapping]
 key = "mapped"
 [[outputs.reload_staging]]
`)
	c := NewStagingConfig()
	_, err := c.Stage(context.Background(), time.Second, path)
	require.NoError(t, err)
	require.Equal(t, count, secretCount.Load())
	require.Equal(t, unlinked, unlinkedSecrets)
	require.Equal(t, []string{"active.conf"}, GetSources())
	input := c.Inputs[0].Input.(*stagingInput)
	require.Nil(t, input.Token.container)
	require.Nil(t, input.Pointer)
	require.Equal(t, "nested name", input.Nested.Name)
	require.Equal(t, "entry name", input.Entries[0].Name)
	require.Empty(t, input.Mapping)
	require.Empty(t, input.Tokens)
	require.Equal(t, "candidate", input.Name)
}

func TestStageRejectsInvalidOptions(t *testing.T) {
	registerStagingPlugins(t)
	creator, registered := parsers.Parsers["influx_upstream"]
	delete(parsers.Parsers, "influx_upstream")
	t.Cleanup(func() {
		if registered {
			parsers.Parsers["influx_upstream"] = creator
		}
	})
	for _, tc := range []struct{ name, data, error string }{
		{"syntax", "[[inputs.reload_staging]", ""},
		{"missing input", "[[inputs.not_compiled]]", "undefined but requested input"},
		{"missing processor", "[[processors.not_compiled]]", "undefined but requested processor"},
		{"missing aggregator", "[[aggregators.not_compiled]]", "undefined but requested aggregator"},
		{"missing secretstore", "[[secretstores.not_compiled]]\nid='unavailable'", "undefined but requested secretstores"},
		{"missing output", "[[outputs.not_compiled]]", "undefined but requested output"},
		{"unknown option", "[[inputs.reload_staging]]\n typo = 1", "but they were not used"},
		{"type mismatch", "[[inputs.reload_staging]]\n count = false", ""},
		{"model type", "[[inputs.reload_staging]]\n alias = 3\n[[outputs.reload_staging]]", "expecting string"},
		{"model array type", "[[inputs.reload_staging]]\n namepass = [3]", "expecting string array"},
		{"secret type", "[[inputs.reload_staging]]\n token = []", "secret setting must be a string"},
		{"ignored field", "[[inputs.reload_staging]]\n ignored = 'secret'", "cannot be set through TOML"},
		{"dash key", "[[inputs.reload_staging]]\n \"-\" = 'secret'", "but they were not used"},
		{"nested typo", "[[inputs.reload_staging]]\n [inputs.reload_staging.nested]\n typo = 1", "but they were not used"},
		{
			"unavailable parser",
			"[[inputs.staging_parser_func]]\n data_format='influx'\n influx_parser_type='upstream'\n[[outputs.reload_staging]]",
			"parser not found",
		},
		{
			"unavailable serializer",
			"[[inputs.reload_staging]]\n[[outputs.reload_staging_serializer]]\n data_format='unavailable'",
			"serializer not found",
		},
		{
			"negative input interval",
			"[[inputs.reload_staging]]\n interval='-1s'\n[[outputs.reload_staging]]",
			"interval must not be negative",
		},
		{
			"input startup_error_behavior",
			"[[inputs.reload_staging]]\n startup_error_behavior='bogus'\n[[outputs.reload_staging]]",
			"invalid 'startup_error_behavior'",
		},
		{
			"output startup_error_behavior",
			"[[inputs.reload_staging]]\n[[outputs.reload_staging]]\n startup_error_behavior='bogus'",
			"invalid 'startup_error_behavior'",
		},
		{
			"negative buffer limit",
			"[[inputs.reload_staging]]\n[[outputs.reload_staging]]\n metric_buffer_limit=-1",
			"must not be negative",
		},
		{
			"zero aggregator period",
			"[[inputs.reload_staging]]\n[[outputs.reload_staging]]\n[[aggregators.reload_staging]]\n period='0s'",
			"period must be positive",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewStagingConfig()
			_, err := c.Stage(context.Background(), time.Second, stagingFile(t, tc.data))
			if err == nil {
				err = c.Validate()
			}
			require.Error(t, err)
			if tc.error != "" {
				require.ErrorContains(t, err, tc.error)
			}
		})
	}
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

func TestSnapshotLoadsFromBytesAfterLocalFileChanges(t *testing.T) {
	registerStagingPlugins(t)
	path := stagingFile(t, "[[inputs.reload_staging]]\ntoken='secret'\nname='original'\n[[outputs.reload_staging]]")
	snapshot, err := NewStagingConfig().Stage(context.Background(), time.Second, path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, []byte("modified"), 0600))
	require.NoError(t, os.Remove(path))
	loaded := NewConfig()
	require.NoError(t, snapshot.Load(loaded))
	defer loaded.Outputs[0].Close()
	loadedInput := loaded.Inputs[0].Input.(*stagingInput)
	require.Equal(t, "original", loadedInput.Name)
	require.NotNil(t, loadedInput.Token.container)
}

func TestStageDoesNotOpenDiskBufferOrResetStatistics(t *testing.T) {
	registerStagingPlugins(t)
	dir := t.TempDir()
	path := stagingFile(t, fmt.Sprintf(`[[inputs.reload_staging]]
 [[outputs.reload_staging]]
 buffer_strategy = "disk_write_through"
 buffer_directory = %q
`, dir))
	staged := NewStagingConfig()
	snapshot, err := staged.Stage(context.Background(), time.Second, path)
	require.NoError(t, err)
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, files)
	active := NewConfig()
	require.NoError(t, snapshot.Load(active))
	defer active.Outputs[0].Close()
	m := metric.New("test", nil, map[string]any{"value": 1}, time.Now())
	active.Outputs[0].AddMetric(m)
	require.Equal(t, 1, active.Outputs[0].BufferLength())
	stat := selfstat.Register("write", "buffer_size", map[string]string{"_id": active.Outputs[0].Config.ID, "output": "reload_staging"})
	stat.Set(17)
	_, err = NewStagingConfig().Stage(context.Background(), time.Second, path)
	require.NoError(t, err)
	require.EqualValues(t, 17, stat.Get())
	require.Equal(t, 1, active.Outputs[0].BufferLength())
}

func TestStageRejectsLaterSource(t *testing.T) {
	registerStagingPlugins(t)
	first := stagingFile(t, "[[inputs.reload_staging]]")
	second := stagingFile(t, "[[outputs.not_compiled]]")
	snapshot, err := NewStagingConfig().Stage(context.Background(), time.Second, first, second)
	require.Nil(t, snapshot)
	require.ErrorContains(t, err, "undefined but requested output")
}

func TestConfigRequestTimeout(t *testing.T) {
	oldInterval := httpLoadConfigRetryInterval
	httpLoadConfigRetryInterval = time.Millisecond
	defer func() { httpLoadConfigRetryInterval = oldInterval }()
	for _, body := range []bool{false, true} {
		t.Run(fmt.Sprintf("body=%v", body), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if body {
					w.WriteHeader(http.StatusOK)
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
			}))
			defer server.Close()
			u, err := url.Parse(server.URL)
			require.NoError(t, err)
			_, _, err = fetchConfigContext(context.Background(), u, 1, 10*time.Millisecond)
			require.Error(t, err)
			require.ErrorContains(t, err, "context deadline exceeded")
		})
	}
}

func TestConfigUnlimitedRetryCancellation(t *testing.T) {
	requested := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		select {
		case requested <- struct{}{}:
		default:
		}
	}))
	defer server.Close()
	u, err := url.Parse(server.URL)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, err := fetchConfigContext(ctx, u, -1, time.Second); done <- err }()
	<-requested
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("configuration retry did not cancel")
	}
}
