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

Project could be built as a usual Maven project:

```
$ mvn clean package -DskipTests
```

Note: subsequent builds could be faster with skiping shading/protobuf compile steps.
See the next section for more info.

# Shading

We shade protos, protobuf and other libraries such as Netty, gRPC, Guava and Hadoop
so that applications using Ratis may use protobuf and other libraries with versions
different from the versions used here.

_Note: RATIS-288 changes how the shaded artifacts are generated, removing them from the
source tree. Developers with local checkouts prior to this change will need to manually
remove the directories `ratis-proto-shaded/src/main/java` and
`ratis-hadoop-shaded/src/main/java`._

By default, protobuf compilation and shaded jar creation are executed for every build.

For developers who wish to skip protobuf generation and shaded jar creation because they
are aware that they have not been modified, they can be disabled with the `skipShade` property.
```
$ mvn package -DskipTests -DskipShade
```

When the `skipShade` property is given, Maven will inspect your local Maven repository for
the most recent version of `ratis-proto-shaded` (or `ratis-hadoop-shaded`), reaching out to
Maven central when you have no local copy. You may need to run a `mvn install` prior to
attempting to use the `skipShade` property to ensure that you have a version of the artifact
available for your use.
```
$ mvn install -DskipTests
```

For developers familiar with the `skipCleanShade` option, this is no longer necessary. Maven's
local repository is acting as a cache instead of the current working copy of your repository.
`mvn clean` can be used to safely clean all temporary build files, without impacting your
use of the `skipShade` option.

Unit tests can also be executed with the `skipShade` option:
```
$ mvn package -DskipShade
```

## What packages are shaded?

| Original packages                   | Shaded packages                                              |
| ------------------------------------|--------------------------------------------------------------|
| `com.google.common`                 | `org.apache.ratis.shaded.com.google.common`                  |
| `com.google.protobuf`               | `org.apache.ratis.shaded.com.google.protobuf`                |
| `com.google.thirdparty.publicsuffix`| `org.apache.ratis.shaded.com.google.thirdparty.publicsuffix` |
| `io.grpc`                           | `org.apache.ratis.shaded.io.grpc`                            |
| `io.netty`                          | `org.apache.ratis.shaded.io.netty`                           |
| `org.apache.hadoop.ipc.protobuf`    | `org.apache.ratis.shaded.org.apache.hadoop.ipc.protobuf`     |

The compiled protocol-buffer definitions in this `ratis-proto-shaded` are stored in the
`org.apache.ratis.shaded.proto` Java package.
