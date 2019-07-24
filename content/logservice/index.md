---
title: LogService
---
<!---
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

The Ratis LogService is an distributed, log implementation built on top of Apache
Ratis. The LogService is a "recipe" on top of Apache Ratis, providing a higher-level
API as compared to Ratis itself. The LogService provides the ability to create named,
durable, append-only data structures with the ability to perform linear reads.

Like Ratis, the LogService is designed to be embedded into another application as
a library, as opposed to a standalone daemon. On a confusing note, there are Java
daemons provided for the LogService, but these are solely to be used for testing.


* [Testing]({{< ref "testing/index.md" >}})
* [Log Lifecycle]({{< ref "lifecycle.md" >}})
* [Security]({{< ref "security/index.md" >}})
