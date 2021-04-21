# bee-repair

Utility used to perform bee migrations.

Migrations are classified by different codenames. These codenames refer to different versions the commands are used for.

## himalaya

These set of migrations are used to fix deprecated Swarm references prior to v0.5.3. All files/folders uploaded need to be repaired using this tool to be able to ensure access to data going ahead.

Subcommands available:

```
Content uploads untill v0.5.3 are written on the swarm network in an older format. This utility is used to repair them by updating to newer format. In order for the references prior to v0.5.3 to be available on nodes running v0.5.4 or up, this utility needs to be used.

Example:

        $ bee-repair himalaya file 2387e8e7d8a48c2a9339c97c1dc3461a9a7aa07e994c5cb8b38fd7c1b3e6ea48
        > 94434d3312320fab70428c39b79dffb4abc3dbedf3e1562384a61ceaf8a7e36b

        $ bee-repair himalaya directory 2387e8e7d8a48c2a9339c97c1dc3461a9a7aa07e994c5cb8b38fd7c1b3e6ea48
        > 94434d3312320fab70428c39b79dffb4abc3dbedf3e1562384a61ceaf8a7e36b

Usage:
   himalaya [command]

Available Commands:
  directory   Repair a directory entry
  file        Repair a file entry

Flags:
      --encrypt       use encryption
  -h, --help          help for himalaya
      --host string   api host (default "127.0.0.1")
      --info string   log verbosity level 0=silent, 1=error, 2=warn, 3=info, 4=debug, 5=trace (default "0")
      --pin           pin the repaired content
      --port int      api port (default 1633)
      --ssl           use ssl

Use " himalaya [command] --help" for more information about a command.

```
