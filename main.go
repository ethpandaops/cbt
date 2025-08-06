// Package main is the entry point for the cbt application
package main

import (
	"github.com/ethpandaops/cbt/cmd"

	_ "github.com/lib/pq"
)

func main() {
	cmd.Execute()
}
