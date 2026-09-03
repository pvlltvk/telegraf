//go:generate ../../../tools/readme_config_includer/generator
//go:build !linux

package rtc

import (
	_ "embed"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

//go:embed sample.conf
var sampleConfig string

type RTC struct {
	Log telegraf.Logger `toml:"-"`
}

func (*RTC) SampleConfig() string {
	return sampleConfig
}

func (r *RTC) Init() error {
	r.Log.Warn("Current platform is not supported")
	return nil
}

func (*RTC) Gather(_ telegraf.Accumulator) error { return nil }

func init() {
	inputs.Add("rtc", func() telegraf.Input {
		return &RTC{}
	})
}
