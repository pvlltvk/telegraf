# URL-Based Config Behavior

## Objective

Define the retry and reload behavior of remote URLs that are passed as config to
Telegraf. In terms of retry, currently Telegraf will attempt to load a remote
URL three times and then exit. In terms of reload, Telegraf does not have the
capability to reload remote URL based configs. This spec seeks to allow for
options for the user to further these capabilities.

Configuration acquisition and validation failures during reload must leave the
running agent collecting with its existing configuration.

## Keywords

config, error, retry, reload

## Overview

Telegraf allows for loading configurations from local files, directories, and
files via a URL. In order to allow situations where a configuration file is not
yet available or due to a flaky network, the first proposal is to introduce a
new CLI flag: `--config-url-retry-attempts`. This flag would continue to default
to three and would specify the number of retries to attempt to get a remote URL
during the initial startup of Telegraf.

```sh
--config-url-retry-attempts=3   Number of times to attempt to obtain a remote
                                configuration via a URL during startup. Set to
                                -1 for unlimited attempts.
```

These attempts would block Telegraf from starting up completely until success or
until we have run out of attempts and exit.

Once Telegraf is up and running, users can use the `--watch-config` flag to enable
watching local files for changes and if/when changes are made, then reload
Telegraf with the new configuration. For remote URLs, the
`--config-url-watch-interval` flag sets an internal timer that checks for updates
to URL-based configuration files.

```sh
--config-url-watch-interval=0s  Time duration to check for updates to URL based
                                configuration files. Disabled by default.
```

At each interval, Telegraf would send an HTTP HEAD request to the configuration
URL, here is an example curl HEAD request and output:

```sh
$ curl --head http://localhost:8000/config.toml
HTTP/1.0 200 OK
Server: SimpleHTTP/0.6 Python/3.12.3
Date: Mon, 29 Apr 2024 18:18:56 GMT
Content-type: application/octet-stream
Content-Length: 1336
Last-Modified: Mon, 29 Apr 2024 11:44:19 GMT
```

Store the last-modified value when first obtaining the file and compare the value
at each interval. No need to parse the value, just store the raw string. If
there is a difference, trigger a reload.

If anything other than a 2xx response code is returned from the HEAD request,
Telegraf prints a warning message and retries at the next interval. Telegraf
will continue to run the existing configuration with no change.

If the value of last-modified is empty, Telegraf disables automatic checking for
this configuration URL and prints a warning message once about the missing field.

### Reload Validation and Request Timeout

Configuration GET and HEAD requests have a per-request timeout, including body
reads. The `--config-url-timeout` flag defaults to `30s` and must be positive.
Requests and retry waits observe process cancellation.

Reload acquires and validates the complete replacement configuration while the
existing agent continues running. Fetch, TOML parsing, unknown plugin or option,
and static validation failures reject the replacement without stopping
collection. Watchers remain available for a subsequent corrected configuration.

Activation uses the validated configuration content without re-reading files or
re-fetching URLs. Activation failures remain fatal. The remote watcher advances
its last-modified baseline only after successful activation.

## Relevant Issues

* Configuration capabilities to retry for loading config via URL #[8854][]
* Telegraf reloads URL-based/remote config on a specified interval #[8730][]

[8854]: https://github.com/influxdata/telegraf/issues/8854
[8730]: https://github.com/influxdata/telegraf/issues/8730
