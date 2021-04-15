// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"os"

	"github.com/ethersphere/bee-repair/himalaya"
	"github.com/spf13/cobra"
)

func main() {
	c := &cobra.Command{
		Short:        "Used to repair broken swarm references",
		SilenceUsage: true,
	}

	himalaya.InitCommands(c)

	c.SetOutput(c.OutOrStdout())
	err := c.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
