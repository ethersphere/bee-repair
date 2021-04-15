// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"os"

	cmdfile "github.com/ethersphere/bee-repair/file"
	"github.com/ethersphere/bee-repair/repair"
	"github.com/ethersphere/bee/pkg/logging"
	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/spf13/cobra"
)

const (
	defaultMimeType     = "application/octet-stream"
	limitMetadataLength = swarm.ChunkSize
)

var (
	host      string // flag variable, http api host
	port      int    // flag variable, http api port
	ssl       bool   // flag variable, uses https for api if set
	verbosity string // flag variable, debug level
	encrypted bool   // flag variable, uses encryption
	logger    logging.Logger
)

type stdOutProgressUpdater struct {
	cmd *cobra.Command
}

func (s *stdOutProgressUpdater) Update(msg string) {
	s.cmd.Println(msg)
}

var fileRepair = &cobra.Command{
	Use:   "file <reference>",
	Short: "Repair a file entry",
	Long: `Repairs a file entry by adding all the required metadata in the new format.

Example:

	$ bee-repair file 2387e8e7d8a48c2a9339c97c1dc3461a9a7aa07e994c5cb8b38fd7c1b3e6ea48
	> 94434d3312320fab70428c39b79dffb4abc3dbedf3e1562384a61ceaf8a7e36b

The input is the hex representation of the swarm hash passed as argument, the result is a new hash which should be used to query the file from the swarm network.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		addr, err := swarm.ParseHexAddress(args[0])
		if err != nil {
			return err
		}
		newReference, err := repair.FileRepair(
			cmd.Context(),
			addr,
			repair.WithHTTPConfig(host, port, ssl),
			repair.WithLogger(logger),
			repair.WithEncryption(encrypted),
			repair.WithProgressUpdater(&stdOutProgressUpdater{cmd}),
		)
		if err != nil {
			return err
		}
		cmd.Println("Repaired file reference. New reference " + newReference.String())
		return nil
	},
}

var directoryRepair = &cobra.Command{
	Use:   "directory <reference>",
	Short: "Repair a directory entry",
	Long: `Repairs a directory entry by adding all the required metadata in the new format.

Example:

	$ bee-repair directory 2387e8e7d8a48c2a9339c97c1dc3461a9a7aa07e994c5cb8b38fd7c1b3e6ea48
	> 94434d3312320fab70428c39b79dffb4abc3dbedf3e1562384a61ceaf8a7e36b

The input is the hex representation of the swarm hash passed as argument, the result is a new hash which should be used to query the directory from the swarm network.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		addr, err := swarm.ParseHexAddress(args[0])
		if err != nil {
			return err
		}
		newReference, err := repair.DirectoryRepair(
			cmd.Context(),
			addr,
			repair.WithHTTPConfig(host, port, ssl),
			repair.WithLogger(logger),
			repair.WithEncryption(encrypted),
			repair.WithProgressUpdater(&stdOutProgressUpdater{cmd}),
		)
		if err != nil {
			return err
		}
		cmd.Println("Repaired directory reference. New reference " + newReference.String())
		return nil
	},
}

func main() {
	c := &cobra.Command{
		Short: "Used to repair broken swarm references",
		Long: `Used to repair broken swarm references.

Example:

	$ bee-repair file 2387e8e7d8a48c2a9339c97c1dc3461a9a7aa07e994c5cb8b38fd7c1b3e6ea48
	> 94434d3312320fab70428c39b79dffb4abc3dbedf3e1562384a61ceaf8a7e36b

	$ bee-repair directory 2387e8e7d8a48c2a9339c97c1dc3461a9a7aa07e994c5cb8b38fd7c1b3e6ea48
	> 94434d3312320fab70428c39b79dffb4abc3dbedf3e1562384a61ceaf8a7e36b`,
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			logger, err = cmdfile.SetLogger(cmd, verbosity)
			return err
		},
	}

	c.PersistentFlags().StringVar(&host, "host", "127.0.0.1", "api host")
	c.PersistentFlags().IntVar(&port, "port", 1633, "api port")
	c.PersistentFlags().BoolVar(&ssl, "ssl", false, "use ssl")
	c.PersistentFlags().StringVar(&verbosity, "info", "0", "log verbosity level 0=silent, 1=error, 2=warn, 3=info, 4=debug, 5=trace")
	c.PersistentFlags().BoolVar(&encrypted, "encrypt", false, "use encryption")

	c.AddCommand(fileRepair)
	c.AddCommand(directoryRepair)

	c.SetOutput(c.OutOrStdout())
	err := c.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
