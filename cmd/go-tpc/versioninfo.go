package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/pingcap/go-tpc/pkg/util"
)

func printVersion() {
	fmt.Println("Git Commit Hash:", util.BuildHash)
	fmt.Println("UTC Build Time:", util.BuildTS)
	fmt.Println("Release version:", util.ReleaseVersion)
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
