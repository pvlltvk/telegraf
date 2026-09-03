//go:build linux

package rtc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/testutil"
)

func TestInitInvalidDevice(t *testing.T) {
	plugin := &RTC{
		Devices: []string{"../rtc0"},
	}
	require.ErrorContains(t, plugin.Init(), "invalid device")
}

func TestGather(t *testing.T) {
	tests := []struct {
		name     string
		devices  []string
		expected []telegraf.Metric
	}{
		{
			name: "all devices",
			expected: []telegraf.Metric{
				metric.New(
					"rtc",
					map[string]string{"device": "rtc0"},
					map[string]interface{}{"offset_sec": int64(5)},
					time.Unix(0, 0),
					telegraf.Gauge,
				),
				metric.New(
					"rtc",
					map[string]string{"device": "rtc1"},
					map[string]interface{}{"offset_sec": int64(15)},
					time.Unix(0, 0),
					telegraf.Gauge,
				),
			},
		},
		{
			name:    "selected device",
			devices: []string{"rtc1"},
			expected: []telegraf.Metric{
				metric.New(
					"rtc",
					map[string]string{"device": "rtc1"},
					map[string]interface{}{"offset_sec": int64(15)},
					time.Unix(0, 0),
					telegraf.Gauge,
				),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := &RTC{
				Devices:   tt.devices,
				classPath: "testdata/normal/class/rtc",
				now:       func() time.Time { return time.Unix(1700000005, 0) },
			}
			require.NoError(t, plugin.Init())

			var acc testutil.Accumulator
			require.NoError(t, plugin.Gather(&acc))
			require.Empty(t, acc.Errors)

			testutil.RequireMetricsEqual(t, tt.expected, acc.GetTelegrafMetrics(), testutil.IgnoreTime())
		})
	}
}

func TestGatherErrors(t *testing.T) {
	tests := []struct {
		name      string
		devices   []string
		classPath string
		expected  string
	}{
		{
			name:      "missing device",
			devices:   []string{"rtc9"},
			classPath: "testdata/normal/class/rtc",
			expected:  "reading device \"rtc9\" failed",
		},
		{
			name:      "invalid time",
			classPath: "testdata/invalid/class/rtc",
			expected:  "parsing time \"garbage\" failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := &RTC{
				Devices:   tt.devices,
				classPath: tt.classPath,
				now:       func() time.Time { return time.Unix(1700000005, 0) },
			}
			require.NoError(t, plugin.Init())

			var acc testutil.Accumulator
			require.NoError(t, plugin.Gather(&acc))
			require.Len(t, acc.Errors, 1)
			require.ErrorContains(t, acc.Errors[0], tt.expected)
			require.Empty(t, acc.GetTelegrafMetrics())
		})
	}
}

func TestGatherNoDevices(t *testing.T) {
	plugin := &RTC{classPath: t.TempDir()}
	require.NoError(t, plugin.Init())

	var acc testutil.Accumulator
	require.ErrorContains(t, plugin.Gather(&acc), "no RTC device found")
}
