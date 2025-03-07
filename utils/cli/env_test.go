package cli_test

import (
	"testing"

	"github.com/jessekempf/mister-kafka/utils/cli"
	"github.com/stretchr/testify/assert"
)

func TestGetEnvStr(t *testing.T) {
	t.Run("gets an env var as a string", func(t *testing.T) {
		t.Setenv("_FOOBAR", t.Name())

		assert.Equal(t, cli.GetEnvStr("_FOOBAR"), t.Name())
	})

	t.Run("panics if the env var is missing", func(t *testing.T) {
		assert.Panics(t, func() { cli.GetEnvStr("_WHAT_IS_THE_LIKELIHOOD_THIS_IS_SET_LOL") })
	})
}
