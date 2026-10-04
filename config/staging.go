package config

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/influxdata/toml/ast"
)

type configSource struct {
	path  string
	table *ast.Table
}

// Snapshot holds the source contents and environment substitutions validated
// during staging. Loading it never reads files or contacts configuration servers.
type Snapshot struct {
	sources      []configSource
	LastModified map[string]string
}

// NewStagingConfig creates a configuration that validates options without
// initializing plugins, acquiring buffers, or publishing statistics and sources.
func NewStagingConfig() *Config {
	c := createConfig(false)
	c.validationOnly = true
	return c
}

func (c *Config) Stage(ctx context.Context, timeout time.Duration, paths ...string) (*Snapshot, error) {
	snapshot := &Snapshot{LastModified: make(map[string]string)}
	for _, path := range paths {
		file, err := readConfigFile(ctx, path, c.Agent.ConfigURLRetryAttempts, timeout)
		if err != nil {
			return nil, fmt.Errorf("loading config file %s failed: %w", path, err)
		}
		table, err := parseConfig(file.data)
		if err != nil {
			return nil, fmt.Errorf("loading config file %s failed: %w", path, err)
		}
		snapshot.sources = append(snapshot.sources, configSource{path: path, table: table})
		if file.remote {
			snapshot.LastModified[path] = file.lastModified
		}
		if err := c.loadConfigTable(cloneTable(table), path); err != nil {
			return nil, fmt.Errorf("loading config file %s failed: %w", path, err)
		}
	}
	if c.hasErrs() {
		return nil, c.firstErr()
	}
	if err := c.finishLoading(); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *Snapshot) Load(c *Config) error {
	for _, source := range s.sources {
		if err := c.loadConfigTable(cloneTable(source.table), source.path); err != nil {
			return fmt.Errorf("loading config file %s failed: %w", source.path, err)
		}
		publishSource(source.path)
	}
	return c.finishLoading()
}

func cloneTable(table *ast.Table) *ast.Table {
	cloned := *table
	cloned.Fields = make(map[string]any, len(table.Fields))
	for key, value := range table.Fields {
		cloned.Fields[key] = cloneNode(value)
	}
	return &cloned
}

func cloneNode(node any) any {
	switch v := node.(type) {
	case *ast.Table:
		return cloneTable(v)
	case []*ast.Table:
		cloned := make([]*ast.Table, len(v))
		for i, table := range v {
			cloned[i] = cloneTable(table)
		}
		return cloned
	case *ast.KeyValue:
		cloned := *v
		cloned.Value = cloneNode(v.Value).(ast.Value)
		return &cloned
	case *ast.Array:
		cloned := *v
		cloned.Value = make([]ast.Value, len(v.Value))
		for i, value := range v.Value {
			cloned.Value[i] = cloneNode(value).(ast.Value)
		}
		return &cloned
	case *ast.String:
		cloned := *v
		return &cloned
	case *ast.Integer:
		cloned := *v
		return &cloned
	case *ast.Float:
		cloned := *v
		return &cloned
	case *ast.Boolean:
		cloned := *v
		return &cloned
	case *ast.Datetime:
		cloned := *v
		return &cloned
	default:
		return node
	}
}

func (c *Config) unmarshalTable(table *ast.Table, target any) error {
	if !c.validationOnly {
		return c.toml.UnmarshalTable(table, target)
	}
	cloned := cloneTable(table)
	if _, err := c.removeSecrets(cloned, reflect.TypeOf(target)); err != nil {
		return err
	}
	return c.toml.UnmarshalTable(cloned, target)
}

// Secret unmarshalling owns protected memory and global bookkeeping. Validate
// its shape separately, then omit those values from the staging-only decoder.
func (c *Config) removeSecrets(node any, typ reflect.Type) (bool, error) {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ == reflect.TypeFor[Secret]() {
		kv, ok := node.(*ast.KeyValue)
		if !ok {
			return false, errors.New("secret setting must be a string")
		}
		if _, ok := kv.Value.(*ast.String); !ok {
			return false, fmt.Errorf("line %d: secret setting must be a string", kv.Line)
		}
		return true, nil
	}
	switch v := node.(type) {
	case *ast.Table:
		for key, child := range v.Fields {
			var childType reflect.Type
			switch typ.Kind() {
			case reflect.Struct:
				childType = c.fieldType(typ, key)
			case reflect.Map:
				childType = typ.Elem()
			}
			if childType == nil {
				continue
			}
			remove, err := c.removeSecrets(child, childType)
			if err != nil {
				return false, err
			}
			if remove {
				delete(v.Fields, key)
			}
		}
	case []*ast.Table:
		if typ.Kind() == reflect.Slice {
			for _, table := range v {
				if _, err := c.removeSecrets(table, typ.Elem()); err != nil {
					return false, err
				}
			}
		}
	case *ast.KeyValue:
		if array, ok := v.Value.(*ast.Array); ok && typ.Kind() == reflect.Slice {
			var containsSecrets bool
			for _, value := range array.Value {
				remove, err := c.removeSecrets(&ast.KeyValue{Value: value, Line: v.Line}, typ.Elem())
				if err != nil {
					return false, err
				}
				containsSecrets = containsSecrets || remove
			}
			if containsSecrets {
				array.Value = nil
			}
		}
	}
	return false, nil
}

func (c *Config) fieldType(typ reflect.Type, key string) reflect.Type {
	named := make(map[string]reflect.Type)
	automatic := make(map[string]reflect.Type)
	var visit func(reflect.Type)
	visit = func(typ reflect.Type) {
		for field := range typ.Fields() {
			if field.PkgPath != "" && !field.Anonymous {
				continue
			}
			tag := strings.TrimSpace(strings.SplitN(field.Tag.Get("toml"), ",", 2)[0])
			if field.Anonymous && field.Type.Kind() == reflect.Struct && tag == "" {
				visit(field.Type)
				continue
			}
			if tag == "" || tag == "-" {
				name := c.toml.NormFieldName(typ, field.Name)
				if _, exists := automatic[name]; exists || tag == "-" {
					automatic[name] = nil
				} else {
					automatic[name] = field.Type
				}
			} else {
				if _, exists := named[tag]; exists {
					named[tag] = nil
				} else {
					named[tag] = field.Type
				}
			}
		}
	}
	visit(typ)
	if field, found := named[key]; found {
		return field
	}
	return automatic[c.toml.NormFieldName(typ, key)]
}

// Validate checks settings that do not require plugin initialization.
func (c *Config) Validate() error {
	if c.Agent.MetricBatchSize < 0 || c.Agent.MetricBufferLimit < 0 {
		return errors.New("agent metric_batch_size and metric_buffer_limit must not be negative")
	}
	for _, input := range c.Inputs {
		if input.Config.Interval < 0 {
			return fmt.Errorf("input %s interval must not be negative", input.Config.Name)
		}
		switch input.Config.StartupErrorBehavior {
		case "", "error", "retry", "ignore", "probe":
		default:
			return fmt.Errorf("input %s: invalid 'startup_error_behavior' setting %q", input.Config.Name, input.Config.StartupErrorBehavior)
		}
		switch input.Config.TimeSource {
		case "", "metric", "collection_start", "collection_end":
		default:
			return fmt.Errorf("input %s: invalid 'time_source' setting %q", input.Config.Name, input.Config.TimeSource)
		}
	}
	for _, output := range c.Outputs {
		if output.Config.MetricBatchSize < 0 || output.Config.MetricBufferLimit < 0 {
			return fmt.Errorf("output %s metric_batch_size and metric_buffer_limit must not be negative", output.Config.Name)
		}
		if output.Config.FlushInterval < 0 {
			return fmt.Errorf("output %s flush_interval must not be negative", output.Config.Name)
		}
		switch output.Config.StartupErrorBehavior {
		case "", "error", "retry", "ignore":
		default:
			return fmt.Errorf("output %s: invalid 'startup_error_behavior' setting %q", output.Config.Name, output.Config.StartupErrorBehavior)
		}
	}
	for _, aggregator := range c.Aggregators {
		if aggregator.Config.Period <= 0 {
			return fmt.Errorf("aggregator %s period must be positive", aggregator.Config.Name)
		}
	}
	return nil
}
