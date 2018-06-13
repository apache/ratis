<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Building

Apache Ratis uses Apache Maven to build the artifacts.
It is required to have Maven 3.3.9 or later.
Apache Ratis is written in Java 8.
Therefore, it as well requires Java 8 or later.

Project could be built as a usual maven project:

```
$ mvn clean package -DskipTests
```

Note: subsequent builds could be faster with skiping shading/protobuf compile steps.
See the next section for more info.

# Shading

We shade protos, protobuf and other libraries such as Netty, gRPC, Guava and Hadoop
so that applications using Ratis may use protobuf and other libraries with versions
different from the versions used here.

Ratis requires the shaded sources for compilation.
The generated sources are stored in the following directories.
```
ratis-proto-shaded/src/main/java/
ratis-hadoop-shaded/src/main/java/
```
They are not checked-in to git though.

By default protobuf compilation and shading are triggered at every build. To make more faster the
additional builds, you can turn them off:
```
$ mvn package -DskipTests -DskipShade
```

During the clean lifecycle all the shaded classes are also deleted. You can clean the
compiled files but keep the shaded classes with the following command:

```
$ mvn clean -DskipCleanShade
```

Then, the unit tests can be run by the following commands.

```
$ mvn -DskipShade test
```

## What are shaded?

| Original packages                   | Shaded packages                                              |
| ------------------------------------|--------------------------------------------------------------|
| `com.google.common`                 | `org.apache.ratis.shaded.com.google.common`                  |
| `com.google.protobuf`               | `org.apache.ratis.shaded.com.google.protobuf`                |
| `com.google.thirdparty.publicsuffix`| `org.apache.ratis.shaded.com.google.thirdparty.publicsuffix` |
| `io.grpc`                           | `org.apache.ratis.shaded.io.grpc`                            |
| `io.netty`                          | `org.apache.ratis.shaded.io.netty`                           |
| `org.apache.hadoop.ipc.protobuf`    | `org.apache.ratis.shaded.org.apache.hadoop.ipc.protobuf`     |

The protos defined in this project are stored in the `org.apache.ratis.shaded.proto` package.
