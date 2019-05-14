# Overview

This document describes Thunder's logging system. This system
features output that lists the log domain and severity in a clear manner, allows the log level both
globally and for specific subsystems to be changed while the server is running, and is simple to
use.

The output looks like

        2018/06/12 10:47:31 INFO: [/Bouncer/Server]: bncrServer.go:113: expecting chainid 19
        2018/06/12 10:47:31 INFO: [/Bouncer/Client]: bncrClient.go:82: starting
        2018/06/12 10:47:31 INFO: [/Bouncer/Client/Conn]: bncrClient.go:107: Connected from 127.0.0.1:48876 to 127.0.0.1:8887
        2018/06/12 10:47:31 INFO: [/Bouncer/Server]: bncrServer.go:162: start listening
        2018/06/12 10:47:31 INFO: [/Bouncer/Server]: bncrServer.go:165: bouncer listening on port [::]:8886
        2018/06/12 10:47:31 INFO: [/Bouncer]: bouncer.go:154: ackChannelHandler starting
        2018/06/12 10:47:31 INFO: [/Bouncer]: bouncer.go:139: txChannelHandler starting
        2018/06/12 10:47:38 INFO: [/Bouncer/Server/Conn] 1: bncrServer.go:205: Connection from [::1]:49920
        2018/06/12 10:47:38 INFO: [/Bouncer]: bouncer.go:145: got tx id 1/0 from txChannel
        2018/06/12 10:47:38 INFO: [/Bouncer/Client]: bncrClient.go:326: send txact 1/0
        2018/06/12 10:47:38 INFO: [/Bouncer]: bouncer.go:145: got tx id 2/1

# Usage

To use a logger, create an instance of a Lgr object for each subsystem
(client, server, connection, etc.).  These are organized hierarchically, so that it is possible
to globally display all log messages of level warning or above, but for /Bouncer/Server display
info and above.  In code this looks like

        svr.lgr = bouncer.Lgr.ChildLgr("Server")

to log something:

        svr.lgr.Info("bouncer listening on port %s", svr.listenIpPort)

which produces

        2018/06/12 10:47:31 INFO: [/Bouncer/Server]: bncrServer.go:165: bouncer listening on port [::]:8886

## Tagged Loggers

Sometimes sub-systems can make their logs extra useful by adding fixed tags to each
log line. For example, connection loggers can add information about remote host, or committee can
add its own committee id which can be useful in tests which run multiple committee in same process. 
To build tagged loggers, use:

        committee.lgr = lgr.NewLgrT("Committee", commId)
        OR
        conn.lgr = svr.lgr.NewChildLgrT("Conn", remoteAddr)  // svr.lgr from above example

Log line for tagged loggers will look like: 

        2018/06/12 10:47:38 INFO: [/Bouncer/Server/Conn(10.0.0.1:12345)]: bncrServer.go:252: tx check failure: invalid transaction v, r, s values

This is a format that's easy to scan visually, and offers a lot of control.  At a previous company
we used something like this, and it was very useful to be able to temporarily log more data from
specific areas, and then turn it back to default, after collecting some data.

It uses a single log.Logger instance to write the data to the output writer.

# Output levels

Output levels are controlled by calling lgr.SetLevel, as in

        lgr.SetLevel("/Bouncer/Client", lgr.LvlWarning)

# Load/Save logging level specs

The current set of log level specs can be saved by calling

        err := WriteLogCfg(os.Stdout) //or any io.Writer

and can be read via

        err := ReadLogCfg(os.Stdin) //or any io.Reader
