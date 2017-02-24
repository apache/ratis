# Building
Ratis uses Apache Maven for the builds. A 3.0+ version of Maven is required as well as 
at least Java-1.8.

When Ratis is build for the first time, shaded protobuf files needs to be generated first 
which happens only in the mvn package phase. That is why you should run:
`$ mvn install -DskipTests`
the first time the sources are checked out first. After doing that mvn compile or mvn test
can be used as normal.

# Raft Proto Shaded

We shade protos, protobuf and other libraries such as Netty, gRPC and Hadoop so that
applications using Raft may use protobuf and other libraries with versions different 
from the versions used here.

The library requires the shaded sources for compilation. The generated sources are stored in
`ratis-proto-shaded/src/main/java/`. They are not checked-in git though. 

If you want to force-compile the proto files (for example after changing them), you should 
run with
$ mvn install -Dcompile-protobuf

## What are shaded?

| Original packages                 | Shaded packages                                          |
| ----------------------------------|----------------------------------------------------------|
| `com.google.protobuf`             | `org.apache.ratis.shaded.com.google.protobuf`             |
| `io.grpc`                         | `org.apache.ratis.shaded.io.grpc`                         |
| `io.netty.handler.codec.protobuf` | `org.apache.ratis.shaded.io.netty.handler.codec.protobuf` |
| `org.apache.hadoop.ipc.protobuf`  | `org.apache.ratis.shaded.org.apache.hadoop.ipc.protobuf`  |

The protos defined in this project are stored in the `org.apache.ratis.shaded.proto` package.

# How to deploy

To publish, use the following settings.xml file ( placed in ~/.m2/settings.xml )
```
<settings>
<servers>
  <server>
    <id>apache.releases.https</id>
    <username>ratis_committer</username>
    <password>********</password>
  </server>
  
  <server>
    <id>apache.snapshots.https</id>
    <username>ratis_committer</username>
    <password>********</password>
  </server>
  
</servers>
</settings>
```

Then use
```
$ mvn deploy
(or)
$ mvn -s /my/path/settings.xml deploy
```
We also use release profile for building the release
```
$ mvn install -Prelease -Papache-release
```