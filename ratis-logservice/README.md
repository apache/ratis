# Ratis LogService


## Example shell

Build:
```bash
$ mvn install
```

Change to logservice dectory:
```bash
$ cd ratis-log-service
```

Launch Metadata daemons:
```bash
$ mvn exec:java -Dexec.mainClass=org.apache.ratis.logservice.server.MetadataServer -Dexec.args="-p 9991 -d $HOME/logservice1 -h localhost -q localhost:9991,localhost:9992,localhost:9993"
```
```bash
$ mvn exec:java -Dexec.mainClass=org.apache.ratis.logservice.server.MetadataServer -Dexec.args="-p 9992 -d $HOME/logservice2 -h localhost -q localhost:9991,localhost:9992,localhost:9993"
```
```bash
$ mvn exec:java -Dexec.mainClass=org.apache.ratis.logservice.server.MetadataServer -Dexec.args="-p 9993 -d $HOME/logservice3 -h localhost -q localhost:9991,localhost:9992,localhost:9993"
```

Above, we have started three daemons that will form a quorum to act as the "LogService Metadata". They will track what
logs exist and the RAFT qs which service those logs.

Launch Worker daemons:
```bash
$ mvn exec:java -Dexec.mainClass=org.apache.ratis.logservice.server.LogServer -Dexec.args="-p 9951 -d $HOME/worker1 -h localhost -q localhost:9991,localhost:9992,localhost:9993"
```
```bash
$ mvn exec:java -Dexec.mainClass=org.apache.ratis.logservice.server.LogServer -Dexec.args="-p 9952 -d $HOME/worker2 -h localhost -q localhost:9991,localhost:9992,localhost:9993"
```
```bash
$ mvn exec:java -Dexec.mainClass=org.apache.ratis.logservice.server.LogServer -Dexec.args="-p 9953 -d $HOME/worker3 -h localhost -q localhost:9991,localhost:9992,localhost:9993"
```

Now, we have started three daemons which can service a single LogStream. They will rep to the Metadata q,
and the Metadata quorum will choose three of them to form a RAFT quorum to "host" a single Log.

Note: the `q` option here references to the Metadata quorum, not the worker quorum as is the case for the Metadata daemons.

Launch client:
```bash
$ mvn exec:java -Dexec.mainClass=org.apache.ratis.logservice.shell.LogServiceShell -Dexec.args="-q localhost:9990,localhost:9991,localhost:9992"
```

This command will launch an interactive shell that you can use to interact with the system.
