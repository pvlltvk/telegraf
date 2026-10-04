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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/outputs"
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

func registerStagingPlugins(t *testing.T) {
	t.Helper()
	inputs.Inputs["reload_staging"] = func() telegraf.Input { return &stagingInput{} }
	outputs.Outputs["reload_staging"] = func() telegraf.Output { return &stagingOutput{} }
	t.Cleanup(func() { delete(inputs.Inputs, "reload_staging"); delete(outputs.Outputs, "reload_staging") })
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
	for _, tc := range []struct{ name, data, error string }{
		{"syntax", "[[inputs.reload_staging]", ""},
		{"missing input", "[[inputs.not_compiled]]", "undefined but requested input"},
		{"missing processor", "[[processors.not_compiled]]", "undefined but requested processor"},
		{"missing aggregator", "[[aggregators.not_compiled]]", "undefined but requested aggregator"},
		{"missing secretstore", "[[secretstores.not_compiled]]\nid='unavailable'", "undefined but requested secretstores"},
		{"missing output", "[[outputs.not_compiled]]", "undefined but requested output"},
		{"unknown option", "[[inputs.reload_staging]]\n typo = 1", "but they were not used"},
		{"model string type", "[[inputs.reload_staging]]\n alias = 3", "expecting string"},
		{"model number type", "[[outputs.reload_staging]]\n metric_batch_size = false", "expecting int"},
		{"model array type", "[[inputs.reload_staging]]\n namepass = [3]", "expecting string array"},
		{"wrong type", "[[inputs.reload_staging]]\n count = false", ""},
		{"secret type", "[[inputs.reload_staging]]\n token = []", "secret setting must be a string"},
		{"ignored field", "[[inputs.reload_staging]]\n ignored = 'secret'", "cannot be set through TOML"},
		{"dash key", "[[inputs.reload_staging]]\n \"-\" = 'secret'", "but they were not used"},
		{"nested typo", "[[inputs.reload_staging]]\n [inputs.reload_staging.nested]\n typo = 1", "but they were not used"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewStagingConfig().Stage(context.Background(), time.Second, stagingFile(t, tc.data))
			require.Error(t, err)
			if tc.error != "" {
				require.ErrorContains(t, err, tc.error)
			}
		})
	}
}

func TestSnapshotUsesValidatedEnvironmentAndSources(t *testing.T) {
	registerStagingPlugins(t)
	t.Setenv("RELOAD_STAGING_NAME", "validated")
	first := stagingFile(t, `[[inputs.reload_staging]]
 name = "${RELOAD_STAGING_NAME}"
 [[outputs.reload_staging]]
`)
	second := stagingFile(t, `[[inputs.reload_staging]]
 name = "second"
`)
	c := NewStagingConfig()
	snapshot, err := c.Stage(context.Background(), time.Second, first, second)
	require.NoError(t, err)
	t.Setenv("RELOAD_STAGING_NAME", "changed")
	require.NoError(t, os.Remove(first))
	require.NoError(t, os.Remove(second))
	loaded := NewConfig()
	require.NoError(t, snapshot.Load(loaded))
	defer loaded.Outputs[0].Close()
	require.Len(t, loaded.Inputs, 2)
	require.Equal(t, "validated", loaded.Inputs[0].Input.(*stagingInput).Name)
	require.Equal(t, "second", loaded.Inputs[1].Input.(*stagingInput).Name)
	require.Equal(t, []string{first, second}, GetSources())
	require.ErrorContains(t, loaded.Inputs[0].Init(), "activation only")
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
