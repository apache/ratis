<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Ratis LogService

The Ratis LogService is an distributed, log implementation built on top of Apache Ratis. The LogService provides the
ability to create named, durable, append-only data structures with the ability to perform linear reads.

## Launching the LogService

The LogService is compose of two Ratis quorums: the metadata quorum and the log quorum. These can be launched manually
or via docker-compose.

First, the project must be built:
```bash
$ mvn clean package -DskipTests
```

Then, the service can be launched.

### Manual

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

Now, we have started three daemons which can service a single LogStream. They will register to the Metadata quorum,
and the Metadata quorum will choose three of them to form a RAFT quorum to "host" a single Log.

Note: the `q` option here references to the Metadata quorum, not the worker quorum as is the case for the Metadata daemons.

Then, the LogService interactive shell can be used to interact with the software:
```bash
$ mvn exec:java -Dexec.mainClass=org.apache.ratis.logservice.shell.LogServiceShell -Dexec.args="-q localhost:9991,localhost:9992,localhost:9993"
```

## Docker

Similarly, a full quorum can be started by building a docker container and then start the docker-compose cluster:
```bash
$ cd ratis-logservice && mvn package assembly:single -DskipTests
$ ./build-docker.sh
$ docker-compose up
```

Then, a client container can be launched to connect to the cluster:
```bash
$ ./client-env.sh
$ ./bin/shell -q master1.logservice.ratis.org:9999,master2.logservice.ratis.org:9999,master3.logservice.ratis.org:9999
```

Or, you can launch the verification tool to generate load on the cluster:
```bash
$ ./client-env.sh
$ ./bin/load-test -q master1.logservice.ratis.org:9999,master2.logservice.ratis.org:9999,master3.logservice.ratis.org:9999
```

`client-env.sh` launches a Docker container that can communicate with the LogService cluster running from
`docker-compose`. You can do this by hand, but take care that the correct network is provided when launching your
container.

## Service configuration

The log service loads configuration values from a file named logservice.xml. The service looks up this file in an application class path.
All service configuration options are described in logservice-example.xml file.
