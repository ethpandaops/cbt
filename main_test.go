package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMain_Help runs main() with the --help flag. Cobra's --help handling
// returns no error, so cmd.Execute() returns normally and main() does not exit.
func TestMain_Help(t *testing.T) {
	origArgs := os.Args
	t.Cleanup(func() { os.Args = origArgs })

	os.Args = []string{"cbt", "--help"}

	assert.NotPanics(t, main)
}
