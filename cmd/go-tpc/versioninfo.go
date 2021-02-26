package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func printVersion() {
	fmt.Println("Git Commit Hash:", commit)
	fmt.Println("UTC Build Time:", date)
	fmt.Println("Release version:", version)
}

func registerVersionInfo(root *cobra.Command) {
	cmd := &cobra.Command{
		Use: "version",
		Run: func(cmd *cobra.Command, args []string) {
			printVersion()
		},
	}
	root.AddCommand(cmd)
}
