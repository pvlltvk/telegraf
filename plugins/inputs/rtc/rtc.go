//go:generate ../../../tools/readme_config_includer/generator
//go:build linux

package rtc

import (
	_ "embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

//go:embed sample.conf
var sampleConfig string

// path to the RTC class directory in sysfs
const rtcClassPath = "/sys/class/rtc"

type RTC struct {
	Devices []string `toml:"devices"`

	classPath string
	now       func() time.Time
}

func (*RTC) SampleConfig() string {
	return sampleConfig
}

func (r *RTC) Init() error {
	for _, device := range r.Devices {
		if filepath.Base(device) != device {
			return fmt.Errorf("invalid device %q", device)
		}
	}

	if r.classPath == "" {
		r.classPath = rtcClassPath
	}
	if r.now == nil {
		r.now = time.Now
	}

	return nil
}

func (r *RTC) Gather(acc telegraf.Accumulator) error {
	devices := r.Devices
	if len(devices) == 0 {
		matches, err := filepath.Glob(filepath.Join(r.classPath, "rtc*"))
		if err != nil {
			return fmt.Errorf("discovering RTC devices failed: %w", err)
		}
		if len(matches) == 0 {
			return errors.New("no RTC device found")
		}
		for _, match := range matches {
			devices = append(devices, filepath.Base(match))
		}
	}

	for _, device := range devices {
		rtcTime, err := r.readTime(device)
		if err != nil {
			acc.AddError(fmt.Errorf("reading device %q failed: %w", device, err))
			continue
		}

		tags := map[string]string{
			"device": device,
		}
		fields := map[string]interface{}{
			"offset_sec": r.now().Unix() - rtcTime,
		}
		acc.AddGauge("rtc", fields, tags)
	}

	return nil
}

// readTime returns the time of the given RTC device in seconds since epoch
func (r *RTC) readTime(device string) (int64, error) {
	buf, err := os.ReadFile(filepath.Join(r.classPath, device, "since_epoch"))
	if err != nil {
		return 0, err
	}

	raw := strings.TrimSpace(string(buf))
	rtcTime, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing time %q failed: %w", raw, err)
	}

	return rtcTime, nil
}

func init() {
	inputs.Add("rtc", func() telegraf.Input {
		return &RTC{}
	})
}
