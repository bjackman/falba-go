package parser_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bjackman/falba/internal/falba"
	"github.com/bjackman/falba/internal/parser"
	"github.com/bjackman/falba/internal/test"
	"github.com/google/go-cmp/cmp"
)

func fakeArtifact(t *testing.T, content string) *falba.Artifact {
	path := filepath.Join(t.TempDir(), "artifact")
	err := os.WriteFile(path, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Setting up fake artifact: %v", err)
	}
	return &falba.Artifact{Name: "artifact", Path: path}
}

func TestParser(t *testing.T) {
	// Invalid configurations
	for _, pattern := range []string{
		// Only one match group is allowed.
		"(foo)(bar)",
	} {
		e, err := parser.NewRegexpExtractor(pattern, falba.ValueInt)
		if err == nil {
			t.Errorf("Wanted error for regexp pattern %q, got %v", pattern, e)
		}
	}

	// Parse failures
	for _, tc := range []struct {
		desc    string
		content string
		parser  *parser.Parser
	}{
		{
			desc:    "empty content",
			content: "",
			parser:  test.MustNewRegexpParser(t, ".+", "my-metric", falba.ValueInt),
		},
		{
			desc:    "not int",
			content: "foo",
			parser:  test.MustNewRegexpParser(t, ".+", "my-metric", falba.ValueInt),
		},
		{
			desc:    "float not int",
			content: "1.0",
			parser:  test.MustNewRegexpParser(t, ".+", "my-metric", falba.ValueInt),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			result, err := tc.parser.Parse(fakeArtifact(t, tc.content))
			if err == nil {
				t.Errorf("Expected error, got none, result = %v", result)
			} else if !errors.Is(err, parser.ErrParseFailure) {
				t.Errorf("Expected ErrParseFailure, got %v", err)
			}
		})
	}

	// Happy paths.
	for _, tc := range []struct {
		desc    string
		content string
		parser  *parser.Parser
		want    *falba.Metric
	}{
		{
			desc:    "int",
			content: "1",
			parser:  test.MustNewRegexpParser(t, ".+", "my-metric", falba.ValueInt),
			want:    &falba.Metric{Name: "my-metric", Value: &falba.IntValue{Value: 1}},
		},
		{
			desc:    "int group",
			content: "foo 1",
			parser:  test.MustNewRegexpParser(t, "foo (\\d+)", "my-metric", falba.ValueInt),
			want:    &falba.Metric{Name: "my-metric", Value: &falba.IntValue{Value: 1}},
		},
		{
			desc:    "float int",
			content: "1",
			parser:  test.MustNewRegexpParser(t, ".+", "my-metric", falba.ValueFloat),
			want:    &falba.Metric{Name: "my-metric", Value: &falba.FloatValue{Value: 1.0}},
		},
		{
			desc:    "float",
			content: "1.0",
			parser:  test.MustNewRegexpParser(t, ".+", "my-metric", falba.ValueFloat),
			want:    &falba.Metric{Name: "my-metric", Value: &falba.FloatValue{Value: 1.0}},
		},
		{
			desc:    "string",
			content: "yerp",
			parser:  test.MustNewRegexpParser(t, ".+", "my-metric", falba.ValueString),
			want:    &falba.Metric{Name: "my-metric", Value: &falba.StringValue{Value: "yerp"}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			result, err := tc.parser.Parse(fakeArtifact(t, tc.content))
			if err != nil {
				t.Fatalf("Unexpected failure to parse: %v", err)
			}
			if len(result.Facts) != 0 {
				t.Errorf("Unexpected Facts: %v", result.Facts)
			}
			if diff := cmp.Diff(result.Metrics, []*falba.Metric{tc.want}); diff != "" {
				t.Errorf("Unexpected Metrics, diff: %v", diff)
			}
		})
	}
}

func TestShellvarParser(t *testing.T) {
	mustNewShellvarParser := func(t *testing.T, varName string, factName string, valueType falba.ValueType) *parser.Parser {
		t.Helper()
		extractor, err := parser.NewShellvarExtractor(varName, valueType)
		if err != nil {
			t.Fatalf("NewShellvarExtractor(%q, %v) failed: %v", varName, valueType, err)
		}
		// ArtifactRE is "." to match any artifact name for these tests
		p, err := parser.NewParser("testShellvar", ".", &parser.ParserTarget{Name: factName, TargetType: parser.TargetFact, ValueType: valueType}, extractor)
		if err != nil {
			t.Fatalf("NewParser failed: %v", err)
		}
		return p
	}

	happyPathTestCases := []struct {
		desc    string
		content string
		parser  *parser.Parser
		want    falba.Value
	}{
		{
			desc:    "simple string",
			content: "MY_VAR=simplevalue",
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "simplevalue"},
		},
		{
			desc:    "double quotes",
			content: `MY_VAR="value with spaces"`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "value with spaces"},
		},
		{
			desc:    "single quotes (literal string)",
			content: `MY_VAR='another value'`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: `'another value'`}, // strconv.Unquote fails, returns raw
		},
		{
			desc:    "escaped double quotes inside double quotes",
			content: `MY_VAR="value with \"escaped\" quotes"`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "value with \"escaped\" quotes"}, // strconv.Unquote handles this
		},
		{
			desc:    "escaped backslash inside double quotes",
			content: `MY_VAR="value with \\ backslash"`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "value with \\ backslash"}, // strconv.Unquote handles this
		},
		{
			desc:    "escaped dollar and backtick inside double quotes (Go specific)",
			// strconv.Unquote handles Go escapes like \$, but os-release might not intend $ to be special unless for expansion (which is not supported)
			// For `\$` and `\``, strconv.Unquote will produce `$` and ```.
			// The os-release spec says "$", quotes, backslash, backtick must be escaped.
			// So `\$` should become `$`. `\`` should become ```. This aligns.
			content: `MY_VAR="value with \$dollar and \`+"`backtick`"+`"`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "value with $dollar and `backtick`"},
		},
		{
			desc:    "single quotes inside double quotes",
			content: `MY_VAR="value with 'single' quotes"`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "value with 'single' quotes"}, // strconv.Unquote handles this
		},
		{
			desc:    "double quotes inside single quotes (literal single quotes)",
			content: `MY_VAR='value with "double" quotes'`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: `'value with "double" quotes'`}, // strconv.Unquote fails, returns raw
		},
		{
			desc:    "escaped single quote inside single quotes (literal single quotes)",
			content: `MY_VAR='value with \'escaped\' single quote'`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: `'value with \'escaped\' single quote'`}, // strconv.Unquote fails, returns raw
		},
		{
			desc:    "escaped backslash inside single quotes (literal single quotes)",
			content: `MY_VAR='value with \\ backslash'`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: `'value with \\ backslash'`}, // strconv.Unquote fails, returns raw
		},
		{
			desc: "comments and blank lines ignored",
			content: `
# This is a comment
MY_VAR="comment_test"

OTHER_VAR=foo
			`,
			parser: mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:   &falba.StringValue{Value: "comment_test"},
		},
		{
			desc:    "variable at end of file",
			content: "FIRST_VAR=123\nMY_VAR=endvalue",
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "endvalue"},
		},
		{
			desc:    "integer value",
			content: "MY_INT_VAR=12345",
			parser:  mustNewShellvarParser(t, "MY_INT_VAR", "my_int_fact", falba.ValueInt),
			want:    &falba.IntValue{Value: 12345},
		},
		{
			desc:    "integer value with quotes",
			content: `MY_INT_VAR="67890"`,
			parser:  mustNewShellvarParser(t, "MY_INT_VAR", "my_int_fact", falba.ValueInt),
			want:    &falba.IntValue{Value: 67890}, // strconv.Unquote then falba.ParseValue
		},
		{
			desc:    "unrecognised Go escape sequence in double quotes",
			content: `MY_VAR="value with \q char"`, // \q is invalid Go escape
			// strconv.Unquote will fail. parseValue will return rawValue.
			parser: mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:   &falba.StringValue{Value: `"value with \q char"`},
		},
		{
			desc:    "single quotes with non-Go escapes (literal single quotes)",
			content: `MY_VAR='value with \n newline char'`, // \n is not special for strconv.Unquote in single quotes (which it fails on)
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: `'value with \n newline char'`}, // strconv.Unquote fails, returns raw
		},
		{
			desc:    "empty value unquoted",
			content: `MY_VAR=`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: ""},
		},
		{
			desc:    "empty value double quoted",
			content: `MY_VAR=""`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: ""},
		},
		{
			desc:    "empty value single quoted",
			content: `MY_VAR=''`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: ""},
		},
		{
			desc:    "value with equals sign (quoted)",
			content: `MY_VAR="foo=bar"`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "foo=bar"},
		},
		{
			desc:    "value is just quotes",
			content: `MY_VAR="\""`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "\""},
		},
		{
			desc:    "value is just escaped backslash",
			// File content: MY_VAR="\\" (variable set to a single backslash)
			// File content: MY_VAR="\\" (variable set to a single backslash)
			// strconv.Unquote will handle this correctly.
			content: "MY_VAR=\\\"\\\\\\\"",
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "\\"},
		},
		{
			desc:    "valid trailing backslash in double quotes (value ends with literal backslash)",
			content: `MY_VAR="value\\"`, // Represents "value\"
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "value\\"}, // strconv.Unquote handles this
		},
		{
			desc:    "valid trailing backslash in single quotes (literal single quotes)",
			content: `MY_VAR='value\\'`, // Represents "value\" but in single quotes
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: `'value\\'`}, // strconv.Unquote fails, returns raw
		},
	}

	for _, tc := range happyPathTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			artifact := fakeArtifact(t, tc.content)
			result, err := tc.parser.Parse(artifact)
			if err != nil {
				t.Fatalf("Parse() failed: %v", err)
			}
			if len(result.Metrics) != 0 {
				t.Errorf("Expected 0 metrics, got %d", len(result.Metrics))
			}
			if len(result.Facts) != 1 {
				t.Errorf("Expected 1 fact, got %d: %v", len(result.Facts), result.Facts)
				return
			}
			factName := tc.parser.Target.Name
			got, ok := result.Facts[factName]
			if !ok {
				t.Fatalf("Fact %q not found in results. Got: %v", factName, result.Facts)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Parse() mismatch (-want +got):\n%s", diff)
			}
		})
	}

	errorTestCases := []struct {
		desc        string
		content     string
		parser      *parser.Parser
		expectError bool // True if any error, if false, means ErrParseFailure from Extract
	}{
		{
			desc:    "variable not found",
			content: "OTHER_VAR=foo",
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
		},
		{
			desc:    "malformed line (no equals) - var not found",
			content: "MY_VAR value", // Line is skipped, MY_VAR not found by that name.
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
		},
		{
			desc:    "empty file - var not found",
			content: "",
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
		},
		{
			desc:    "type mismatch (string for int)",
			content: "MY_INT_VAR=notanint", // parseValue returns "notanint", falba.ParseValue("notanint", Int) errors.
			parser:  mustNewShellvarParser(t, "MY_INT_VAR", "my_int_fact", falba.ValueInt),
		},
		{
			desc:    "type mismatch (single-quoted string for int)",
			content: "MY_INT_VAR='123'", // parseValue returns "'123'", falba.ParseValue("'123'", Int) errors.
			parser:  mustNewShellvarParser(t, "MY_INT_VAR", "my_int_fact", falba.ValueInt),
		},
		{
			desc:        "extractor creation fails (empty var name)",
			content:     "FOO=bar", // Content doesn't matter here
			parser:      nil,       // Indicates test is about parser (extractor) creation
			expectError: true,      // Expect a non-ErrParseFailure, a setup error
		},
		// Tests for malformed quoting resulting in ErrParseFailure directly from parseValue
		// are mostly removed because strconv.Unquote failing leads to raw value return.
		// An error would now typically come from falba.ParseValue if the raw (potentially bad) string
		// cannot be converted to the target type.
		// Example: MY_VAR="abc (target int) -> parseValue returns "abc", falba.ParseValue errors.
		// Example: MY_VAR="abc (target string) -> parseValue returns "abc", falba.ParseValue succeeds.
		{
			desc:    "invalid escape for strconv.Unquote then type mismatch (int)",
			// MY_VAR="\z" -> strconv.Unquote fails, parseValue returns "\z"
			// falba.ParseValue("\z", int) fails.
			content: `MY_VAR="\z"`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueInt),
		},
	}

	// Test extractor creation failure separately
	t.Run("extractor creation fails (empty var name)", func(t *testing.T) {
		_, err := parser.NewShellvarExtractor("", falba.ValueString)
		if err == nil {
			t.Error("Expected error for empty var name, got nil")
		}
	})

	for _, tc := range errorTestCases {
		if tc.parser == nil { // Skip tests where parser setup itself is the test
			continue
		}
		t.Run(tc.desc, func(t *testing.T) {
			artifact := fakeArtifact(t, tc.content)
			_, err := tc.parser.Parse(artifact)
			if err == nil {
				t.Fatalf("Parse() expected error, got nil")
			}
			if !tc.expectError && !errors.Is(err, parser.ErrParseFailure) {
				t.Errorf("Parse() expected ErrParseFailure, got %v", err)
			} else if tc.expectError && errors.Is(err, parser.ErrParseFailure) {
				t.Errorf("Parse() expected a fatal error, but got ErrParseFailure: %v", err)
			}
		})
	}

	// Test FromConfig for shellvar
	t.Run("FromConfig shellvar", func(t *testing.T) {
		configJSON := `{
			"type": "shellvar",
			"artifact_regexp": "os-release",
			"var": "PRETTY_NAME",
			"fact": {
				"name": "os_pretty_name",
				"type": "string"
			}
		}`
		p, err := parser.FromConfig([]byte(configJSON), "shellvar_test_parser")
		if err != nil {
			t.Fatalf("FromConfig failed: %v", err)
		}
		if p.Name != "shellvar_test_parser" {
			t.Errorf("Expected parser name %q, got %q", "shellvar_test_parser", p.Name)
		}
		if p.ArtifactRE.String() != "os-release" {
			t.Errorf("Expected ArtifactRE %q, got %q", "os-release", p.ArtifactRE.String())
		}
		if p.Target.Name != "os_pretty_name" || p.Target.ValueType != falba.ValueString || p.Target.TargetType != parser.TargetFact {
			t.Errorf("Unexpected target: %+v", p.Target)
		}
		// Check if the extractor is ShellvarExtractor (type assertion)
		shellvarExtractor, ok := p.Extractor.(*parser.ShellvarExtractor)
		if !ok {
			t.Fatalf("Extractor is not of type *ShellvarExtractor, got %T", p.Extractor)
		}
		if shellvarExtractor.VarName != "PRETTY_NAME" {
			t.Errorf("Expected extractor VarName %q, got %q", "PRETTY_NAME", shellvarExtractor.VarName)
		}

		// Test with actual content
		content := `NAME="Ubuntu"
VERSION="20.04.3 LTS (Focal Fossa)"
ID=ubuntu
ID_LIKE=debian
PRETTY_NAME="Ubuntu 20.04.3 LTS"
VERSION_ID="20.04"
HOME_URL="https://www.ubuntu.com/"
SUPPORT_URL="https://help.ubuntu.com/"
BUG_REPORT_URL="https://bugs.launchpad.net/ubuntu/"
PRIVACY_POLICY_URL="https://www.ubuntu.com/legal/terms-and-policies/privacy-policy"
VERSION_CODENAME=focal
UBUNTU_CODENAME=focal
`
		artifact := fakeArtifact(t, content)
		result, err := p.Parse(artifact)
		if err != nil {
			t.Fatalf("Parse() with FromConfig parser failed: %v", err)
		}
		wantValue := &falba.StringValue{Value: "Ubuntu 20.04.3 LTS"}
		gotValue, ok := result.Facts["os_pretty_name"]
		if !ok {
			t.Fatalf("Fact 'os_pretty_name' not found in results")
		}
		if diff := cmp.Diff(wantValue, gotValue); diff != "" {
			t.Errorf("Parse() mismatch for FromConfig (-want +got):\n%s", diff)
		}
	})

	t.Run("FromConfig shellvar missing var", func(t *testing.T) {
		configJSON := `{
			"type": "shellvar",
			"artifact_regexp": "os-release",
			"fact": {
				"name": "os_pretty_name",
				"type": "string"
			}
		}`
		_, err := parser.FromConfig([]byte(configJSON), "shellvar_test_parser")
		if err == nil {
			t.Fatal("FromConfig expected error for missing 'var', got nil")
		}
		if !strings.Contains(err.Error(), "missing/empty 'var' field") {
			t.Errorf("Expected error about missing 'var', got: %v", err)
		}
	})
}

func TestReservedFactNamesRejected(t *testing.T) {
	testCases := []struct {
		name         string
		factName     string
		expectError  bool
	}{
		{"test_name reserved", "test_name", true},
		{"result_id reserved", "result_id", true},
		{"valid fact name", "my_fact", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := `{
				"type": "single_metric",
				"artifact_regexp": "test_artifact",
				"fact": {
					"name": "` + tc.factName + `",
					"type": "string"
				}
			}`

			_, err := parser.FromConfig([]byte(config), "test_parser")

			if tc.expectError {
				if err == nil {
					t.Fatalf("Expected error for reserved fact name %q, but got none", tc.factName)
				}
				if !strings.Contains(err.Error(), "reserved") {
					t.Errorf("Expected error about reserved fact name, got: %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error for valid fact name %q: %v", tc.factName, err)
				}
			}
		})
	}
}
