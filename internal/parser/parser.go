// Package parser contains logic for parsing metrics and facts from artifacts
package parser

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/PaesslerAG/jsonpath"
	"github.com/bjackman/falba/internal/falba"
)

// ParseResult is just  halper to avoid typing out verbose map and slice biz.
// TODO: This is wack, still figuring out these details  of the data model, so
// probably this type makes no sense anyway.
type ParseResult struct {
	Facts   map[string]falba.Value
	Metrics []*falba.Metric
}

func emptyParseResult() *ParseResult {
	return &ParseResult{
		Facts:   map[string]falba.Value{},
		Metrics: []*falba.Metric{},
	}
}

var ErrParseFailure = errors.New("parse failure")

// An Extractor contains the core logic for reading a value from an artifact.
type Extractor interface {
	fmt.Stringer
	// Parse processes a single Artifact and produces results. If the error
	// returned Is a ErrParseFailure it just means something is unexpected about
	// the Artifact contents, otherwise it means something went completely wrong.
	Extract(artifact *falba.Artifact) (falba.Value, error)
}

type TargetType int

const (
	TargetFact TargetType = iota
	TargetMetric
)

// Describes the thing a parser produces, i.e. a fact or metric.
type ParserTarget struct {
	Name       string
	TargetType TargetType
	ValueType  falba.ValueType
}

func (t *ParserTarget) result(val falba.Value) *ParseResult {
	r := emptyParseResult()
	if t.TargetType == TargetMetric {
		r.Metrics = append(r.Metrics, &falba.Metric{Name: t.Name, Value: val})
	} else {
		r.Facts[t.Name] = val
	}
	return r
}

// A Parser is a bundle of logic for extracting information from Artifacts.
type Parser struct {
	Name string
	// Only produce metrics for artifacts matching this regexp.
	ArtifactRE *regexp.Regexp
	Target     *ParserTarget
	Extractor
}

func NewParser(name string, artifactPattern string, target *ParserTarget, extractor Extractor) (*Parser, error) {
	artifactRE, err := regexp.Compile(artifactPattern)
	if err != nil {
		return nil, fmt.Errorf("compiling artifact regexp pattern %q: %v", artifactPattern, err)
	}

	return &Parser{
		Name:       name,
		ArtifactRE: artifactRE,
		Target:     target,
		Extractor:  extractor,
	}, nil
}

// Parse extract facts and metrics from an artifact.
// TODO: This only supports each parser producing a single metric/fact. I'm
// starting to think this is actually a nice simplification. It's less flexible,
// but isn't the whole point of this design that, if you think you wanna gather
// zillions of facts, you are probably wrong? You only need to extract the ones
// you're actually capable of analysing.
//
// Ahhh right, clarity: Yes, we want the flexibility to output _multiple samples
// of the same metric_. We don't really care about producing multiple different
// facts or metrics, I think.
func (p *Parser) Parse(artifact *falba.Artifact) (*ParseResult, error) {
	if !p.ArtifactRE.MatchString(artifact.Name) {
		return emptyParseResult(), nil
	}
	val, err := p.Extractor.Extract(artifact)
	if err != nil {
		return nil, err
	}
	// TODO: Is it OK that we are kinda forgetting the expected type here?
	return p.Target.result(val), nil
}

// RegexpExtractor is an extractor that uses regexps provided by the user to
// extract facts and metrics.
type RegexpExtractor struct {
	resultType falba.ValueType
	// Currently this just supports extracting a single metric from an artifact.
	// The regexp must have zero or one capture groups. If there's a capture
	// group, the metric is taken from the submatch, otherwise from the match of
	// the full regexp.
	re *regexp.Regexp
}

func NewRegexpExtractor(pattern string, resultType falba.ValueType) (*RegexpExtractor, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("compiling regexp pattern %q: %v", pattern, err)
	}
	if re.NumSubexp() > 1 {
		return nil, fmt.Errorf("regexp %q contained %d sub-expressions, up to 1 is allowed", pattern, re.NumSubexp())
	}
	return &RegexpExtractor{re: re, resultType: resultType}, nil
}

func (e *RegexpExtractor) Extract(artifact *falba.Artifact) (falba.Value, error) {
	content, err := artifact.Content()
	if err != nil {
		return nil, fmt.Errorf("getting artifact content: %v", err)
	}

	matches := e.re.FindAllSubmatch(content, -1)
	if len(matches) == 0 {
		return nil, fmt.Errorf("%w: no matches for %v in %v", ErrParseFailure, e.re, artifact)
	}
	if len(matches) > 1 {
		return nil, fmt.Errorf("%w: multple matches for %v in %v, only one is allowed", ErrParseFailure, e.re, artifact)
	}
	match := matches[0][e.re.NumSubexp()]

	val, err := falba.ParseValue(string(match), e.resultType)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrParseFailure, err)
	}

	return val, nil
}

func (p *RegexpExtractor) String() string {
	return fmt.Sprintf("RegexpExtractor{%v -> %v}", p.re, p.resultType)
}

type JSONPathExtractor struct {
	resultType falba.ValueType
	expression string
}

func NewJSONPathExtractor(expr string, resultType falba.ValueType) (*JSONPathExtractor, error) {
	return &JSONPathExtractor{
		expression: expr,
		resultType: resultType,
	}, nil
}

func (e *JSONPathExtractor) Extract(artifact *falba.Artifact) (falba.Value, error) {
	content, err := artifact.Content()
	if err != nil {
		return nil, fmt.Errorf("getting artifact content: %v", err)
	}
	var obj any
	if err := json.Unmarshal(content, &obj); err != nil {
		return nil, fmt.Errorf("%w: unmarshalling from JSON: %v", ErrParseFailure, err)
	}

	// We'd prefer to pre-compile the JSONPath expression but then evaluating it
	// gies you a gval.Evaluable which I can't be bothered to deal with, I don't
	// know how to get non-scalar objects out of it. So instead we just evaluate
	// it as string "at runtime" which gives us an untyped result we can
	// manually try to squash into the type we want.
	got, err := jsonpath.Get(e.expression, obj)
	if err != nil {
		// I believe this error must mean there's something wrong with the
		// expression, not just that it didn't match anything. So this is fatal.
		return nil, fmt.Errorf("failed to evaluate JSONPath: %v", err)
	}

	var gotVal any
	switch got := got.(type) {
	case []any:
		// JSONPath seems to be weird and annoying when you use its
		// filtering functionality, AFAICS it doesn't have a built-in
		// facility to extract an individual value. So we just allow it to
		// return a slice of length 1.
		if len(got) != 1 {
			return nil, fmt.Errorf("%w: JSONPath returned %d values, expected 1", ErrParseFailure, len(got))
		}
		gotVal = got[0]
	default:
		gotVal = got
	}

	switch e.resultType {
	case falba.ValueInt:
		// JSON doesn't have proper numeric types so we can't actually enforce
		// that the value is an integer. Just squash it into one.
		switch v := gotVal.(type) {
		case float64:
			return &falba.IntValue{Value: int64(v)}, nil
		case int:
			return &falba.IntValue{Value: int64(v)}, nil
		default:
			return nil, fmt.Errorf("%w: JSONPath returned %T, wanted numeric", ErrParseFailure, gotVal)
		}
	case falba.ValueString:
		val, ok := gotVal.(string)
		if !ok {
			return nil, fmt.Errorf("%w: JSONPath returned %T, wanted string", ErrParseFailure, gotVal)
		}
		return &falba.StringValue{Value: val}, nil
	case falba.ValueFloat:
		val, ok := gotVal.(float64)
		if !ok {
			return nil, fmt.Errorf("%w: JSONPath returned %T, wanted float64", ErrParseFailure, gotVal)
		}
		return &falba.FloatValue{Value: val}, nil
	default:
		panic("unimplemented")
	}
}

func (p *JSONPathExtractor) String() string {
	return fmt.Sprintf("JSONPathParser{%q -> %v}", p.expression, p.resultType)
}

type BaseParserConfig struct {
	Type string `json:"type"`
	// Parse the artifact if its path (relative to the artifacts dir) matches
	// this regexp.
	ArtifactRegexp string `json:"artifact_regexp"`
	// Specify either the metric to produce, or the fact to produce.
	Metric *struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"metric"`
	Fact *struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"fact"`
}

type ShellvarParserConfig struct {
	BaseParserConfig
	Var string `json:"var"` // Name of the shell variable to extract
}

func (c *ShellvarParserConfig) ValidateFields() error {
	if err := c.BaseParserConfig.ValidateFields(); err != nil {
		return err
	}
	if c.Var == "" {
		return fmt.Errorf("missing/empty 'var' field for shellvar parser")
	}
	return nil
}

// This just checks if the config structure has the right fields, it doesn't
// check if their content is correct.
func (c *BaseParserConfig) ValidateFields() error {
	if c.Type == "" {
		return fmt.Errorf("missing/empty 'type' field")
	}
	if c.ArtifactRegexp == "" {
		return fmt.Errorf("missing/empty 'artifact_regexp' field")
	}
	if (c.Metric != nil) == (c.Fact != nil) {
		return fmt.Errorf("specify exactly one of 'metric' and 'fact'")
	}
	if c.Metric != nil {
		if c.Metric.Name == "" {
			return fmt.Errorf("missing/empty 'metric.name' field")
		}
		if c.Metric.Type == "" {
			return fmt.Errorf("missing/empty 'metric.type' field")
		}
	} else {
		if c.Fact.Name == "" {
			return fmt.Errorf("missing/empty 'fact.name' field")
		}
		if c.Fact.Type == "" {
			return fmt.Errorf("missing/empty 'fact.type' field")
		}
	}
	return nil
}

type JSONPPathConfig struct {
	BaseParserConfig
	JSONPath string `json:"jsonpath"`
}

func (c *JSONPPathConfig) ValidateFields() error {
	if err := c.BaseParserConfig.ValidateFields(); err != nil {
		return err
	}
	if c.JSONPath == "" {
		return fmt.Errorf("missing/empty 'jsonpath' field")
	}
	return nil
}

// Config for a parser that just reads a single metric from a file, using its
// entire content.
type SingleMetricConfig struct {
	BaseParserConfig
}

// Read a configuration entry for a single parser and return it.
func FromConfig(rawConfig json.RawMessage, name string) (*Parser, error) {
	// First parse the common fields, this enables us to get the type, then we
	// can subsequently parse all the remaining fields.
	var baseConfig BaseParserConfig
	if err := json.Unmarshal(rawConfig, &baseConfig); err != nil {
		return nil, fmt.Errorf("decoding 'type' for parser: %v", err)
	}

	var target ParserTarget
	if baseConfig.Metric != nil {
		valueType, err := falba.ParseValueType(baseConfig.Metric.Type)
		if err != nil {
			return nil, fmt.Errorf("parsing metric type: %v", err)
		}
		target = ParserTarget{
			TargetType: TargetMetric,
			Name:       baseConfig.Metric.Name,
			ValueType:  valueType,
		}
	} else if baseConfig.Fact != nil {
		if falba.IsReservedFactName(baseConfig.Fact.Name) {
			return nil, fmt.Errorf("fact name %q is reserved (%s)", baseConfig.Fact.Name, falba.GetReservedFactNamesString())
		}
		valueType, err := falba.ParseValueType(baseConfig.Fact.Type)
		if err != nil {
			return nil, fmt.Errorf("parsing metric type: %v", err)
		}
		target = ParserTarget{
			TargetType: TargetFact,
			Name:       baseConfig.Fact.Name,
			ValueType:  valueType,
		}
	} else {
		return nil, fmt.Errorf("must specify 'fact.type' or 'value.type'")
	}

	var extractor Extractor

	switch baseConfig.Type {
	case "single_metric":
		decoder := json.NewDecoder(strings.NewReader(string(rawConfig)))
		decoder.DisallowUnknownFields()
		var config SingleMetricConfig
		if err := decoder.Decode(&config); err != nil {
			return nil, fmt.Errorf("decoding single_metric parser config: %v", err)
		}
		if err := config.ValidateFields(); err != nil {
			return nil, fmt.Errorf("invalid %q parser config: %v", baseConfig.Type, err)
		}
		var err error
		extractor, err = NewRegexpExtractor(".+", target.ValueType)
		if err != nil {
			return nil, fmt.Errorf("setting up single-value extractor: %v", err)
		}
	case "jsonpath":
		decoder := json.NewDecoder(strings.NewReader(string(rawConfig)))
		decoder.DisallowUnknownFields()
		var config JSONPPathConfig
		if err := decoder.Decode(&config); err != nil {
			return nil, fmt.Errorf("decoding single_metric parser config: %v", err)
		}
		if err := config.ValidateFields(); err != nil {
			return nil, fmt.Errorf("invalid %q parser config: %v", baseConfig.Type, err)
		}
		var err error
		extractor, err = NewJSONPathExtractor(config.JSONPath, target.ValueType)
		if err != nil {
			return nil, fmt.Errorf("setting up JSONPath extractor: %v", err)
		}
	case "shellvar":
		decoder := json.NewDecoder(strings.NewReader(string(rawConfig)))
		decoder.DisallowUnknownFields()
		var config ShellvarParserConfig
		if err := decoder.Decode(&config); err != nil {
			return nil, fmt.Errorf("decoding shellvar parser config: %v", err)
		}
		if err := config.ValidateFields(); err != nil {
			return nil, fmt.Errorf("invalid %q parser config: %v", baseConfig.Type, err)
		}
		var err error
		extractor, err = NewShellvarExtractor(config.Var, target.ValueType)
		if err != nil {
			return nil, fmt.Errorf("setting up Shellvar extractor: %v", err)
		}
	default:
		return nil, fmt.Errorf("unknown parser type %q", baseConfig.Type)
	}

	return NewParser(name, baseConfig.ArtifactRegexp, &target, extractor)
}
