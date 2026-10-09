package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/v22/daemon"
	"github.com/fatih/color"
	"github.com/influxdata/tail/watch"
	"gopkg.in/tomb.v1"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/agent"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/logger"
	"github.com/influxdata/telegraf/plugins/aggregators"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/parsers"
	"github.com/influxdata/telegraf/plugins/processors"
	"github.com/influxdata/telegraf/plugins/secretstores"
)

var stop chan struct{}

type GlobalFlags struct {
	config                  []string
	configDir               []string
	testWait                int
	configURLRetryAttempts  int
	configURLWatchInterval  time.Duration
	configURLTimeout        time.Duration
	watchConfig             string
	watchInterval           time.Duration
	watchDebounceInterval   time.Duration
	pidFile                 string
	plugindDir              string
	password                string
	oldEnvBehavior          bool
	nonStrictEnvVars        bool
	printPluginConfigSource bool
	test                    bool
	debug                   bool
	once                    bool
	quiet                   bool
	unprotected             bool
}

type WindowFlags struct {
	service             string
	serviceName         string
	serviceDisplayName  string
	serviceRestartDelay string
	serviceAutoRestart  bool
	console             bool
}

type App interface {
	Init(<-chan error, Filters, GlobalFlags, WindowFlags)
	Run() error

	// Secret store commands
	ListSecretStores() ([]string, error)
	GetSecretStore(string) (telegraf.SecretStore, error)
}

type Telegraf struct {
	pprofErr <-chan error
	ready    chan<- struct{}

	inputFilters       []string
	outputFilters      []string
	configFiles        []string
	secretstoreFilters []string

	cfg *config.Config

	GlobalFlags
	WindowFlags
}

func (t *Telegraf) Init(pprofErr <-chan error, f Filters, g GlobalFlags, w WindowFlags) {
	t.pprofErr = pprofErr
	t.inputFilters = f.input
	t.outputFilters = f.output
	t.secretstoreFilters = f.secretstore
	t.GlobalFlags = g
	t.WindowFlags = w

	// Disable secret protection before performing any other operation
	if g.unprotected {
		log.Println("W! Running without secret protection!")
		config.DisableSecretProtection()
	}

	// Set global password
	if g.password != "" {
		config.Password = config.NewSecret([]byte(g.password))
	}

	// Set environment replacement behavior
	config.OldEnvVarReplacement = g.oldEnvBehavior
	config.NonStrictEnvVarHandling = g.nonStrictEnvVars
	config.PrintPluginConfigSource = g.printPluginConfigSource
}

func (t *Telegraf) ListSecretStores() ([]string, error) {
	c, err := t.loadConfiguration()
	if err != nil {
		return nil, err
	}

	ids := make([]string, 0, len(c.SecretStores))
	for k := range c.SecretStores {
		ids = append(ids, k)
	}
	return ids, nil
}

func (t *Telegraf) GetSecretStore(id string) (telegraf.SecretStore, error) {
	t.quiet = true
	c, err := t.loadConfiguration()
	if err != nil {
		return nil, err
	}

	store, found := c.SecretStores[id]
	if !found {
		return nil, errors.New("unknown secret store")
	}

	return store, nil
}

type stagedConfiguration struct {
	snapshot *config.Snapshot
	files    []string
	err      error
}

type localWatcher struct {
	cancel context.CancelFunc
	done   <-chan struct{}
}

type remoteRevision struct {
	path     string
	modified string
}

func requestReload(reload chan<- struct{}) {
	select {
	case reload <- struct{}{}:
	default:
	}
}

func (t *Telegraf) reloadLoop() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGHUP, syscall.SIGTERM)
	defer signal.Stop(signals)
	reload := make(chan struct{}, 1)
	remoteChanges := make(chan remoteRevision)
	var workers sync.WaitGroup
	workers.Go(func() {
		for {
			select {
			case sig := <-signals:
				if sig == syscall.SIGHUP {
					requestReload(reload)
				} else {
					cancel()
					return
				}
			case err := <-t.pprofErr:
				log.Printf("E! pprof server failed: %v", err)
				cancel()
				return
			case <-stop:
				cancel()
				return
			case <-ctx.Done():
				return
			}
		}
	})
	defer func() { cancel(); workers.Wait() }()

	var baselines map[string]string
	if t.cfg == nil {
		staged := t.stageConfiguration(ctx)
		if staged.err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return staged.err
		}
		if ctx.Err() != nil {
			return nil
		}
		if err := t.activateConfiguration(staged); err != nil {
			return err
		}
		baselines = staged.snapshot.LastModified
	}

	if t.ready != nil {
		select {
		case t.ready <- struct{}{}:
		case <-ctx.Done():
			return nil
		}
	}

	localWatchers := make(map[string]localWatcher)
	stopLocalWatcher := func(path string) {
		w := localWatchers[path]
		w.cancel()
		<-w.done
		delete(localWatchers, path)
	}
	var stopRemoteWatching context.CancelFunc
	startRemoteWatching := func() {
		if stopRemoteWatching != nil {
			stopRemoteWatching()
		}
		if t.configURLWatchInterval > 0 && len(baselines) > 0 {
			watchCtx, stop := context.WithCancel(ctx)
			stopRemoteWatching = stop
			acceptedBaselines := baselines
			workers.Go(func() { t.watchRemoteConfigs(watchCtx, remoteChanges, t.configURLWatchInterval, acceptedBaselines) })
		}
	}
	// Watchers of unchanged paths keep running across reloads so that edits made
	// while a replacement is activated are not missed.
	startWatching := func() {
		watched := make(map[string]bool)
		if t.watchConfig != "" {
			for _, path := range append(append(make([]string, 0, len(t.configFiles)+len(t.configDir)), t.configFiles...), t.configDir...) {
				if isURL(path) {
					continue
				}
				if _, err := os.Stat(path); err != nil {
					log.Printf("W! Cannot watch config %s: %v", path, err)
					continue
				}
				watched[path] = true
				if w, ok := localWatchers[path]; ok {
					select {
					case <-w.done:
						w.cancel()
					default:
						continue
					}
				}
				watchCtx, cancel := context.WithCancel(ctx)
				done := make(chan struct{})
				localWatchers[path] = localWatcher{cancel: cancel, done: done}
				go func() {
					defer close(done)
					t.watchLocalConfig(watchCtx, reload, path)
				}()
			}
		}
		for path := range localWatchers {
			if !watched[path] {
				stopLocalWatcher(path)
			}
		}
		startRemoteWatching()
	}
	defer func() {
		if stopRemoteWatching != nil {
			stopRemoteWatching()
		}
		for path := range localWatchers {
			stopLocalWatcher(path)
		}
	}()
	startWatching()

	startAgent := func() (context.CancelFunc, <-chan error) {
		agentCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- t.runAgent(agentCtx) }()
		return stop, done
	}
	stopAgent, agentDone := startAgent()
	defer func() { stopAgent() }()
	var staging <-chan stagedConfiguration
	var stopStaging context.CancelFunc
	startStaging := func() {
		log.Println("I! Loading replacement Telegraf config")
		stageCtx, stop := context.WithCancel(ctx)
		stopStaging = stop
		result := make(chan stagedConfiguration, 1)
		staging = result
		go func() { result <- t.stageConfiguration(stageCtx) }()
	}
	for {
		reloadCh := reload
		remoteCh := remoteChanges
		if staging != nil {
			reloadCh = nil
			remoteCh = nil
		}
		select {
		case <-ctx.Done():
			stopAgent()
			if staging != nil {
				stopStaging()
				<-staging
			}
			err := <-agentDone
			if err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("[telegraf] Error running agent: %w", err)
			}
			return nil
		case err := <-agentDone:
			if staging != nil {
				stopStaging()
				<-staging
			}
			if err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("[telegraf] Error running agent: %w", err)
			}
			return nil
		case <-reloadCh:
			startStaging()
		case revision := <-remoteCh:
			// A change detected while staging may already be part of the
			// accepted snapshot; restarting for it would reload the same config.
			if baselines[revision.path] == revision.modified {
				continue
			}
			startStaging()
		case staged := <-staging:
			stopStaging()
			staging = nil
			if ctx.Err() != nil {
				continue
			}
			if staged.err != nil {
				log.Printf("E! [telegraf] Config reload rejected: %v", staged.err)
				startRemoteWatching()
				continue
			}
			stopAgent()
			if err := <-agentDone; err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("[telegraf] Error stopping agent: %w", err)
			}
			if ctx.Err() != nil {
				return nil
			}
			if err := t.activateConfiguration(staged); err != nil {
				return err
			}
			baselines = staged.snapshot.LastModified
			startWatching()
			stopAgent, agentDone = startAgent()
			log.Println("I! Reloading Telegraf config")
		}
	}
}

func (t *Telegraf) watchLocalConfig(ctx context.Context, reload chan<- struct{}, fConfig string) {
	mytomb := new(tomb.Tomb)
	waitForTeardown := func() {}
	defer func() {
		mytomb.Kill(nil)
		waitForTeardown()
	}()
	var watcher watch.FileWatcher
	if t.watchConfig == "poll" {
		if t.watchInterval > 0 {
			watcher = watch.NewPollingFileWatcherWithDuration(fConfig, t.watchInterval)
		} else {
			watcher = watch.NewPollingFileWatcher(fConfig)
		}
	} else {
		watcher = watch.NewInotifyFileWatcher(fConfig)
	}
	subscribe := func() (*watch.FileChanges, error) {
		changes, err := watcher.ChangeEvents(mytomb, 0)
		if err != nil {
			return nil, err
		}
		// The inotify tracker shares one event channel per path and closes it
		// when any subscription is removed, so a successor must not subscribe
		// until the library has released this one.
		if _, ok := watcher.(*watch.InotifyFileWatcher); ok {
			if events := watch.Events(filepath.Clean(fConfig)); events != nil {
				waitForTeardown = func() {
					for {
						if _, ok := <-events; !ok {
							return
						}
					}
				}
			}
		}
		return changes, nil
	}
	changes, err := subscribe()
	if err != nil {
		log.Printf("E! Error watching config file/directory %q: %s\n", fConfig, err)
		return
	}
	log.Printf("I! Config watcher started for %s\n", fConfig)

	rearm := time.NewTicker(time.Second)
	defer rearm.Stop()
	var needsRearm bool

	// Setup debounce timer
	var reloadTimer *time.Timer
	var reloadPending bool

	if t.watchDebounceInterval > 0 {
		reloadTimer = time.NewTimer(t.watchDebounceInterval)
		if !reloadTimer.Stop() {
			<-reloadTimer.C // Drain if already fired
		}
	}

	// Update resetTimer function:
	resetTimer := func(reason string) {
		log.Printf("%s", reason)

		if t.watchDebounceInterval == 0 {
			// No debouncing - trigger immediately
			requestReload(reload)
			return
		}

		if !reloadPending {
			reloadPending = true
		}

		// Properly drain and reset timer
		if !reloadTimer.Stop() {
			select {
			case <-reloadTimer.C:
			default:
			}
		}
		reloadTimer.Reset(t.watchDebounceInterval)
	}

	for {
		select {
		case <-ctx.Done():
			if reloadTimer != nil {
				reloadTimer.Stop()
			}
			return

		case <-changes.Modified:
			resetTimer(fmt.Sprintf("I! Config file/directory %q modified\n", fConfig))

		case <-changes.Deleted:
			needsRearm = true
			// Use select with timeout instead of blocking wait
			timer := time.NewTimer(time.Second)
			select {
			case <-timer.C:
				// Proceed with file existence check
			case <-ctx.Done():
				timer.Stop()
				return
			}

			var reason string
			if _, err := os.Stat(fConfig); err == nil {
				reason = fmt.Sprintf("I! Config file/directory %q overwritten\n", fConfig)
			} else {
				reason = fmt.Sprintf("W! Config file/directory %q deleted\n", fConfig)
			}
			resetTimer(reason)

		case <-changes.Truncated:
			resetTimer(fmt.Sprintf("I! Config file/directory %q truncated\n", fConfig))

		case <-rearm.C:
			if needsRearm {
				if _, err := os.Stat(fConfig); err != nil {
					continue
				}
				mytomb.Kill(nil)
				mytomb = new(tomb.Tomb)
				newChanges, err := subscribe()
				if err != nil {
					continue
				}
				changes = newChanges
				needsRearm = false
				resetTimer(fmt.Sprintf("I! Config file/directory %q recreated\n", fConfig))
			}

		case <-changes.Created:
			resetTimer(fmt.Sprintf("I! Config directory %q has new file(s)\n", fConfig))

		case <-func() <-chan time.Time {
			if reloadTimer != nil {
				return reloadTimer.C
			}
			// Return a channel that never fires when debouncing is disabled
			return nil
		}():
			if reloadPending {
				log.Printf("I! Debounce period elapsed, triggering config reload for %q\n", fConfig)
				requestReload(reload)
				reloadPending = false
			}

		case <-mytomb.Dying():
			if reloadTimer != nil {
				reloadTimer.Stop()
			}
			log.Printf("I! Config watcher %q ended\n", fConfig)
			return
		}
	}
}

func (t *Telegraf) watchRemoteConfigs(ctx context.Context, changes chan<- remoteRevision, interval time.Duration, baselines map[string]string) {
	lastModified := make(map[string]string, len(baselines))
	for path, modified := range baselines {
		if modified == "" {
			log.Printf("W! Last-Modified header not found, disabling automatic watching for %s", path)
		} else {
			lastModified[path] = modified
		}
	}
	if len(lastModified) == 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for path, baseline := range lastModified {
				requestCtx, cancel := context.WithTimeout(ctx, t.urlTimeout())
				req, err := http.NewRequestWithContext(requestCtx, "HEAD", path, nil)
				if err != nil {
					cancel()
					log.Printf("W! Creating request for config %q failed: %v", path, err)
					continue
				}
				if v, exists := os.LookupEnv("TELEGRAF_CONTROLLER_TOKEN"); exists {
					req.Header.Add("Authorization", "Bearer "+v)
				} else if v, exists := os.LookupEnv("INFLUX_TOKEN"); exists {
					req.Header.Add("Authorization", "Token "+v)
				}
				req.Header.Set("User-Agent", internal.ProductToken())
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					cancel()
					log.Printf("W! Checking config %q failed: %v", path, err)
					continue
				}
				resp.Body.Close()
				cancel()
				if resp.StatusCode < 200 || resp.StatusCode >= 300 {
					log.Printf("W! Checking config %q failed: %s", path, resp.Status)
					continue
				}
				modified := resp.Header.Get("Last-Modified")
				if modified == "" {
					log.Printf("W! Last-Modified header not found, disabling automatic watching for %s", path)
					delete(lastModified, path)
				} else if modified != baseline {
					lastModified[path] = modified
					select {
					case changes <- remoteRevision{path: path, modified: modified}:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

func (t *Telegraf) urlTimeout() time.Duration {
	if t.configURLTimeout == 0 {
		return 30 * time.Second
	}
	return t.configURLTimeout
}

func (t *Telegraf) configure(c *config.Config) {
	c.Agent.Quiet = t.quiet
	c.Agent.ConfigURLRetryAttempts = t.configURLRetryAttempts
	c.OutputFilters = t.outputFilters
	c.InputFilters = t.inputFilters
	c.SecretStoreFilters = t.secretstoreFilters
	c.TestMode = !t.once && (t.test || t.testWait != 0)
}

func (t *Telegraf) stageConfiguration(ctx context.Context) stagedConfiguration {
	c := config.NewStagingConfig()
	t.configure(c)
	files, err := t.configurationFiles()
	if err != nil {
		return stagedConfiguration{err: err}
	}
	snapshot, err := c.Stage(ctx, t.urlTimeout(), files...)
	if err == nil {
		err = t.validateConfiguration(c)
	}
	return stagedConfiguration{snapshot: snapshot, files: files, err: err}
}

func (t *Telegraf) activateConfiguration(staged stagedConfiguration) error {
	config.ResetSecrets()
	c := config.NewConfig()
	t.configure(c)
	if err := staged.snapshot.Load(c); err != nil {
		return err
	}
	t.cfg = c
	t.configFiles = staged.files
	return nil
}

func (t *Telegraf) loadConfiguration() (*config.Config, error) {
	config.ResetSecrets()
	c := config.NewConfig()
	t.configure(c)
	if err := t.getConfigFiles(); err != nil {
		return c, err
	}
	if err := c.LoadAllContext(context.Background(), t.urlTimeout(), t.configFiles...); err != nil {
		return c, err
	}
	return c, nil
}

func (t *Telegraf) getConfigFiles() error {
	files, err := t.configurationFiles()
	if err == nil {
		t.configFiles = files
	}
	return err
}

func (t *Telegraf) configurationFiles() ([]string, error) {
	var configFiles []string

	configFiles = append(configFiles, t.config...)
	for _, fConfigDirectory := range t.configDir {
		files, err := config.WalkDirectory(fConfigDirectory)
		if err != nil {
			return nil, fmt.Errorf("reading config directory failed: %w", err)
		}
		configFiles = append(configFiles, files...)
	}

	// load default config paths if none are found
	if len(configFiles) == 0 {
		defaultFiles, err := config.GetDefaultConfigPath()
		if err != nil {
			return nil, fmt.Errorf("unable to load default config paths: %w", err)
		}
		configFiles = append(configFiles, defaultFiles...)
	}

	return configFiles, nil
}

func (t *Telegraf) validateConfiguration(c *config.Config) error {
	if !t.test && t.testWait == 0 && len(c.Outputs) == 0 {
		return errors.New("no outputs found, probably invalid config file provided")
	}
	if t.plugindDir == "" && len(c.Inputs) == 0 {
		return errors.New("no inputs found, probably invalid config file provided")
	}

	if int64(c.Agent.Interval) <= 0 {
		return fmt.Errorf("agent interval must be positive, found %v", c.Agent.Interval)
	}

	if int64(c.Agent.FlushInterval) <= 0 {
		return fmt.Errorf("agent flush_interval must be positive; found %v", c.Agent.Interval)
	}

	if err := c.Validate(); err != nil {
		return err
	}
	return t.loggingConfiguration(c).Validate()
}

func (t *Telegraf) loggingConfiguration(c *config.Config) *logger.Config {
	return &logger.Config{
		Debug:                   c.Agent.Debug || t.debug,
		Quiet:                   c.Agent.Quiet || t.quiet,
		LogTarget:               c.Agent.LogTarget,
		LogFormat:               c.Agent.LogFormat,
		Logfile:                 c.Agent.Logfile,
		StructuredLogMessageKey: c.Agent.StructuredLogMessageKey,
		RotationInterval:        time.Duration(c.Agent.LogfileRotationInterval),
		RotationMaxSize:         int64(c.Agent.LogfileRotationMaxSize),
		RotationMaxArchives:     c.Agent.LogfileRotationMaxArchives,
		LogWithTimezone:         c.Agent.LogWithTimezone,
	}
}

func (t *Telegraf) runAgent(ctx context.Context) error {
	c := t.cfg
	if err := t.validateConfiguration(c); err != nil {
		return err
	}

	logConfig := t.loggingConfiguration(c)

	if err := logger.SetupLogging(logConfig); err != nil {
		return fmt.Errorf("setting up logging failed: %w", err)
	}

	log.Printf("I! Starting Telegraf %s%s brought to you by InfluxData the makers of InfluxDB", internal.Version, internal.Customized)
	log.Printf("I! Available plugins: %d inputs, %d aggregators, %d processors, %d parsers, %d outputs, %d secret stores",
		len(inputs.Inputs),
		len(aggregators.Aggregators),
		len(processors.Processors),
		len(parsers.Parsers),
		len(outputs.Outputs),
		len(secretstores.SecretStores),
	)
	log.Printf("I! Loaded inputs: %s\n%s", strings.Join(c.InputNames(), " "), c.InputNamesWithSources())
	log.Printf("I! Loaded aggregators: %s\n%s", strings.Join(c.AggregatorNames(), " "), c.AggregatorNamesWithSources())
	log.Printf("I! Loaded processors: %s\n%s", strings.Join(c.ProcessorNames(), " "), c.ProcessorNamesWithSources())
	log.Printf("I! Loaded secretstores: %s\n%s", strings.Join(c.SecretstoreNames(), " "), c.SecretstoreNamesWithSources())
	if !t.once && (t.test || t.testWait != 0) {
		log.Print("W! " + color.RedString("Outputs are not used in testing mode!"))
	} else {
		log.Printf("I! Loaded outputs: %s\n%s", strings.Join(c.OutputNames(), " "), c.OutputNamesWithSources())
	}
	log.Printf("I! Tags enabled: %s", c.ListTags())

	if count, found := c.Deprecations["inputs"]; found && (count[0] > 0 || count[1] > 0) {
		log.Printf("W! Deprecated inputs: %d and %d options", count[0], count[1])
	}
	if count, found := c.Deprecations["aggregators"]; found && (count[0] > 0 || count[1] > 0) {
		log.Printf("W! Deprecated aggregators: %d and %d options", count[0], count[1])
	}
	if count, found := c.Deprecations["processors"]; found && (count[0] > 0 || count[1] > 0) {
		log.Printf("W! Deprecated processors: %d and %d options", count[0], count[1])
	}
	if count, found := c.Deprecations["outputs"]; found && (count[0] > 0 || count[1] > 0) {
		log.Printf("W! Deprecated outputs: %d and %d options", count[0], count[1])
	}
	if count, found := c.Deprecations["secretstores"]; found && (count[0] > 0 || count[1] > 0) {
		log.Printf("W! Deprecated secretstores: %d and %d options", count[0], count[1])
	}

	// Compute the amount of locked memory needed for the secrets
	if !t.GlobalFlags.unprotected {
		required := 3 * c.NumberSecrets * uint64(os.Getpagesize())
		available := getLockedMemoryLimit()
		if required > available {
			required /= 1024
			available /= 1024
			log.Printf("I! Found %d secrets...", c.NumberSecrets)
			msg := fmt.Sprintf("Insufficient lockable memory %dkb when %dkb is required.", available, required)
			msg += " Please increase the limit for Telegraf in your Operating System!"
			log.Print("W! " + color.RedString(msg))
		}
	}
	ag := agent.NewAgent(c)

	// Notify systemd that telegraf is ready
	// SdNotify() only tries to notify if the NOTIFY_SOCKET environment is set, so it's safe to call when systemd isn't present.
	// Ignore the return values here because they're not valid for platforms that don't use systemd.
	// For platforms that use systemd, telegraf doesn't log if the notification failed.
	//nolint:errcheck // see above
	daemon.SdNotify(false, daemon.SdNotifyReady)

	if t.once {
		wait := time.Duration(t.testWait) * time.Second
		return ag.Once(ctx, wait)
	}

	if t.test || t.testWait != 0 {
		wait := time.Duration(t.testWait) * time.Second
		return ag.Test(ctx, wait)
	}

	if t.pidFile != "" {
		f, err := os.OpenFile(t.pidFile, os.O_CREATE|os.O_WRONLY, 0640)
		if err != nil {
			log.Printf("E! Unable to create pidfile: %s", err)
		} else {
			fmt.Fprintf(f, "%d\n", os.Getpid())

			err = f.Close()
			if err != nil {
				return err
			}

			defer func() {
				err := os.Remove(t.pidFile)
				if err != nil {
					log.Printf("E! Unable to remove pidfile: %s", err)
				}
			}()
		}
	}

	return ag.Run(ctx)
}

// isURL checks if string is valid url
func isURL(str string) bool {
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}
