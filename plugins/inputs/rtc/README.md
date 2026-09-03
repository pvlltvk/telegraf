# Real-Time Clock Input Plugin

This plugin compares the time of the hardware [real-time clock (RTC)][rtc]
devices against the system clock. A growing offset indicates a drifting or
failing hardware clock, e.g. due to a depleted battery, which becomes a
problem after reboots when no time synchronization is available.

The RTC time is read via sysfs and thus does not require access to the
`/dev/rtc*` devices or elevated privileges.

⭐ Telegraf v1.40.0
🏷️ hardware, system
💻 linux

[rtc]: https://www.kernel.org/doc/html/latest/admin-guide/rtc.html

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

Plugins support additional global and plugin configuration settings for tasks
such as modifying metrics, tags, and fields, creating aliases, and configuring
plugin ordering. See [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Configuration

```toml @sample.conf
# Compare the hardware real-time clock (RTC) against the system clock.
# This plugin ONLY supports Linux
[[inputs.rtc]]
  ## RTC devices to read from /sys/class/rtc. If empty, all available devices
  ## are collected.
  # devices = []
```

## Metrics

- rtc
  - tags:
    - device (name of the RTC device, e.g. `rtc0`)
  - fields:
    - offset_sec (int64) - Difference between the system clock and the RTC
      in seconds. Positive values indicate the RTC is behind the system clock.

The RTC only provides a resolution of one second, so an offset of `1` or `-1`
is expected during normal operation.

## Example Output

```text
rtc,device=rtc0,host=testvm offset_sec=0i 1761121800000000000
```
