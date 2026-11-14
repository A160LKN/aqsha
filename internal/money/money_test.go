package money_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/A160LKN/aqsha/internal/money"
)

// Граничные значения суммы: ноль и минус запрещены, потолок — включительно.
func TestValidate(t *testing.T) {
	cases := []struct {
		name  string
		tiyn  int64
		valid bool
	}{
		{"минус", -1, false},
		{"ноль", 0, false},
		{"один тиын", 1, true},
		{"обычная сумма", 150_000_00, true},
		{"ровно потолок", money.MaxTiyn, true},
		{"на тиын больше потолка", money.MaxTiyn + 1, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := money.Validate(c.tiyn)
			if c.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
