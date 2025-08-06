package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

var (
	// Release is the current release version
	Release = "dev"
	// GitCommit is the git commit hash
	GitCommit = "none"
	// GOOS is the operating system
	GOOS = runtime.GOOS
	// GOARCH is the architecture
	GOARCH = runtime.GOARCH
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Prints the version of cbt.",
	Long:  `Prints the version of cbt.`,
	Run: func(_ *cobra.Command, _ []string) {
		initCommon()

		fmt.Printf("Version: %s\nCommit: %s\nOS/Arch: %s/%s\n",
			Release, GitCommit, GOOS, GOARCH)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
