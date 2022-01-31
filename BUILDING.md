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

# Thirdparty

All bundled thirdparty dependencies are centralized in the *ratis-thirdparty* module
and the *ratis-thirdparty-misc* module.
These modules are located in a separated repository
but not attached to the core Apache Ratis repository
as they only need to change when one of these dependencies are changed.
All dependencies included in ratis-thirdparty/ratis-thirdparty-misc
must be relocated to a different package to ensure no downstream classpath pollution.

Ratis developers should rely on these relocated thirdparty classes.

As a result of this thirdparty module, there is no need for `skipShade` options in the
build which previously existed because the shaded artifacts that are generated each
build are limited only to the code in Ratis itself.

## What packages are shaded?

| Original packages                   | Shaded packages                                                  |
| ------------------------------------|------------------------------------------------------------------|
| `com.google.common`                 | `org.apache.ratis.thirdparty.com.google.common`                  |
| `com.google.protobuf`               | `org.apache.ratis.thirdparty.com.google.protobuf`                |
| `com.google.thirdparty.publicsuffix`| `org.apache.ratis.thirdparty.com.google.thirdparty.publicsuffix` |
| `io.grpc`                           | `org.apache.ratis.thirdparty.io.grpc`                            |
| `io.netty`                          | `org.apache.ratis.thirdparty.io.netty`                           |

All compiled protocol-buffer definitions in `ratis-proto` are stored in the
`org.apache.ratis.proto` Java package.
