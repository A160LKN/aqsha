package transfer_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/A160LKN/aqsha/internal/transfer"
)

// Таблица переходов: все 9 пар «из → в». Разрешены только pending → completed и pending → failed.
func TestCanTransition(t *testing.T) {
	all := []transfer.Status{transfer.Pending, transfer.Completed, transfer.Failed}
	allowed := map[[2]transfer.Status]bool{
		{transfer.Pending, transfer.Completed}: true,
		{transfer.Pending, transfer.Failed}:    true,
	}
	for _, from := range all {
		for _, to := range all {
			t.Run(fmt.Sprintf("%s→%s", from, to), func(t *testing.T) {
				assert.Equal(t, allowed[[2]transfer.Status{from, to}], transfer.CanTransition(from, to))
			})
		}
	}
}

func TestFinal(t *testing.T) {
	assert.False(t, transfer.Pending.Final())
	assert.True(t, transfer.Completed.Final())
	assert.True(t, transfer.Failed.Final())
}
