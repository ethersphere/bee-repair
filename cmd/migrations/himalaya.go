// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package migrations

import (
	"github.com/ethersphere/bee-repair/internal/repair"
	cmdfile "github.com/ethersphere/bee-repair/pkg/file"
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
			repair.WithAPIStore(host, port, ssl),
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
			repair.WithAPIStore(host, port, ssl),
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

func InitHimalayaCommands(rootCmd *cobra.Command) {
	c := &cobra.Command{
		Use:   "himalaya",
		Short: "Used to repair broken swarm references upto bee v0.5.3",
		Long: `Content uploads untill v0.5.3 are written on the swarm network in an older format. This utility is used to repair them by updating to newer format. In order for the references prior to v0.5.3 to be available on nodes running v0.5.4 or up, this utility needs to be used.

Example:

	$ bee-repair himalaya file 2387e8e7d8a48c2a9339c97c1dc3461a9a7aa07e994c5cb8b38fd7c1b3e6ea48
	> 94434d3312320fab70428c39b79dffb4abc3dbedf3e1562384a61ceaf8a7e36b

	$ bee-repair himalaya directory 2387e8e7d8a48c2a9339c97c1dc3461a9a7aa07e994c5cb8b38fd7c1b3e6ea48
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

	rootCmd.AddCommand(c)
}
