//go:build !custom || inputs || inputs.rtc

package all

import _ "github.com/influxdata/telegraf/plugins/inputs/rtc" // register plugin
