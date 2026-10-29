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

Apache Ratis uses Apache Maven 3.3.9 or later to build the artifacts.
You can use the bundled Maven Wrapper to build without a pre-installed Maven,
by replacing `mvn` with the `mvnw` (*nix) or `mvnw.cmd` (Windows) script in the root path.

Apache Ratis is written in Java 8.
Therefore, it as well requires Java 8 or later.

Project could be built as a usual Maven project:

```
$ mvn clean package -DskipTests
```

Note: subsequent builds could be faster with skiping shading/protobuf compile steps.
See the next section for more info.

## Error Prone

Run [Error Prone](https://errorprone.info/) static analysis with JDK 17 or 21:

```
$ dev-support/checks/errorprone.sh
```

The check compiles main and test sources without running tests. Reports are written to
`target/errorprone`: `output.log` contains the complete build output, `diagnostics.txt`
contains compiler diagnostics, and `summary.txt` contains errors. Warnings do not fail the check.
Generated sources are excluded from Error Prone analysis.

The Maven profile can also be used directly:

```
$ mvn clean test-compile -Perrorprone -DskipTests
```

Normal builds do not enable Error Prone and continue to support Java 8.
To test the check script's report handling, run `bash dev-support/tests/test-errorprone.sh`.

## Infer

Run [Infer](https://fbinfer.com/docs/getting-started) static analysis with JDK 21:

```
$ dev-support/checks/infer.sh
```

The check installs Infer 1.3.0 in `.dev-tools` if `infer` is not already on `PATH`.
Automatic installation supports Linux x86_64 and macOS arm64. The check also requires
`jq` to read Infer's JSON report.

The check performs a clean Maven build with compilation caches disabled, without running tests.
Test and generated sources are excluded from analysis. Reports and diagnostics are written to
`target/infer`, including `report.txt`, `report.json`, `captured-sources.txt`, `output.log`,
and `summary.txt`. CI archives this directory without the large Infer databases in `infer-out`.
Any unsuppressed, uncensored finding fails the check, regardless of severity. Installation, build,
capture, and reporting failures also fail the check. Suppressed findings remain in the JSON report
for review but do not count as failures. There is no baseline allowance for existing findings.

To test the check script's report handling, run `bash dev-support/tests/test-infer.sh`.

### Handling Infer findings

Validate each finding against its trace and the implementation before classifying it as a false positive.
For a confirmed false positive, explain the invariant in a comment and add
`// @infer-ignore ISSUE_TYPE` immediately before the reported line, using the exact issue type from the report.
Add focused tests for the invariant where practical. Do not suppress an entire file or issue type to hide individual findings.

Rerun the check after adding a suppression. Verify that the finding is absent from `report.txt`
and retained in `report.json` with `"suppressed": true`; the summary count excludes suppressed findings.
Leave unconfirmed findings visible and address confirmed bugs separately.

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
