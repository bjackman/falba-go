package parser

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"

	"github.com/bjackman/falba/internal/falba"
)

// ShellvarExtractor extracts a value from a shell-compatible variable assignment file.
type ShellvarExtractor struct {
	VarName    string
	ResultType falba.ValueType
}

// NewShellvarExtractor creates a new ShellvarExtractor.
func NewShellvarExtractor(varName string, resultType falba.ValueType) (*ShellvarExtractor, error) {
	if varName == "" {
		return nil, fmt.Errorf("variable name cannot be empty")
	}
	return &ShellvarExtractor{
		VarName:    varName,
		ResultType: resultType,
	}, nil
}

// Extract implements the Extractor interface.
func (e *ShellvarExtractor) Extract(artifact *falba.Artifact) (falba.Value, error) {
	content, err := artifact.Content()
	if err != nil {
		return nil, fmt.Errorf("getting artifact content: %v", err)
	}

	reader := strings.NewReader(string(content))
	scanner := bufio.NewScanner(reader)
	lineNumber := 0

	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())

		if line == "" || strings.HasPrefix(line, "#") {
			continue // Skip blank lines and comments
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			// Malformed line, could log this if a logger was available
			continue
		}

		key := strings.TrimSpace(parts[0])
		if key == e.VarName {
			rawValue := strings.TrimSpace(parts[1])
			value, err := e.parseValue(rawValue)
			if err != nil {
				return nil, fmt.Errorf("%w: parsing raw value for variable %q on line %d: %v", ErrParseFailure, e.VarName, lineNumber, err)
			}

			parsedVal, err := falba.ParseValue(value, e.ResultType)
			if err != nil {
				return nil, fmt.Errorf("%w: converting value %q for variable %q to type %v: %v", ErrParseFailure, value, e.VarName, e.ResultType, err)
			}
			return parsedVal, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading artifact content: %v", err)
	}

	return nil, fmt.Errorf("%w: variable %q not found in artifact %q", ErrParseFailure, e.VarName, artifact.Name)
}

// parseValue handles quotes using strconv.Unquote.
// strconv.Unquote handles double quotes and Go-style escapes.
// It does not handle single quotes directly as per shell standards.
// If Unquote fails, we assume it's an unquoted string or a single-quoted string
// to be taken literally (including quotes if present and not parsable by Unquote).
func (e *ShellvarExtractor) parseValue(rawValue string) (string, error) {
	if len(rawValue) == 0 {
		return "", nil
	}

	// Try strconv.Unquote first. This primarily handles double-quoted strings
	// with Go-style escapes.
	unquoted, err := strconv.Unquote(rawValue)
	if err == nil {
		return unquoted, nil
	}

	// If strconv.Unquote fails, it might be an unquoted string,
	// or a single-quoted string that strconv.Unquote doesn't understand as shell does.
	// For simplicity, as per user direction, we are not implementing full manual
	// single-quote parsing here.
	// The os-release spec says: "Variable assignment values must be enclosed in
	// double or single quotes if they include spaces, semicolons or other special
	// characters ... Assignments that do not include these special characters may be
	// enclosed in quotes too, but this is optional."
	// If it's single quoted like 'foo bar', strconv.Unquote fails.
	// We could strip single quotes manually if present, but that also gets complex with escapes.
	// For now, if Unquote fails, return rawValue. This means single-quoted values
	// will be returned with their quotes. This is a known limitation of this simplified approach.
	// Example: VARIANT_ID='debian test' -> result will be "'debian test'"
	// Example: VARIANT_ID=debian -> result will be "debian"
	return rawValue, nil
}

// String implements the fmt.Stringer interface.
func (e *ShellvarExtractor) String() string {
	return fmt.Sprintf("ShellvarExtractor{VarName: %q, ResultType: %v}", e.VarName, e.ResultType)
}

// Ensure ShellvarExtractor implements Extractor.
var _ Extractor = &ShellvarExtractor{}
