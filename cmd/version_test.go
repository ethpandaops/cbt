package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersionCommand(t *testing.T) {
	// Invoke the version command's Run handler directly to exercise its output.
	assert.NotPanics(t, func() {
		versionCmd.Run(versionCmd, nil)
	})

	assert.Equal(t, "version", versionCmd.Use)
}
