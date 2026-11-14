package config

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"time"

	"go.yaml.in/yaml/v3"
)

type Duration time.Duration

var daysRe = regexp.MustCompile(`^[0-9]+d$`)

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return fmt.Errorf("line %d: duration must be a string", n.Line)
	}
	if daysRe.MatchString(n.Value) {
		days, err := strconv.ParseInt(n.Value[:len(n.Value)-1], 10, 64)
		if err != nil || days > math.MaxInt64/int64(24*time.Hour) {
			return fmt.Errorf("line %d: duration %q is too large", n.Line, n.Value)
		}
		*d = Duration(time.Duration(days) * 24 * time.Hour)
		return nil
	}
	v, err := time.ParseDuration(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: invalid duration %q: use Go syntax (90s, 10m, 24h) or whole days (7d)", n.Line, n.Value)
	}
	*d = Duration(v)
	return nil
}
