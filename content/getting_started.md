---
title: Getting started
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

Ratis is a [Raft](https://raft.github.io/) protocol *library* in Java. It's not a standalone server application like Zookeeper or Consul.

### Examples

To demonstrate how to use Ratis from the code, Please look at the following examples.

 * [Arithmetic example](https://github.com/apache/incubator-ratis/tree/master/ratis-examples/src/main/java/org/apache/ratis/examples/arithmetic): This is a simple distributed calculator that replicates the values defined and allows user to perform arithmetic operations on these replicated values.

 * [FileStore example](https://github.com/apache/incubator-ratis/tree/master/ratis-examples/src/main/java/org/apache/ratis/examples/filestore): This is an example of using Ratis for reading and writing files.

<!-- TODO: We should have the following as documentation in the github.  -->

The source code of the examples could be found in the
[ratis-examples](https://github.com/apache/incubator-ratis/blob/master/ratis-examples/) sub-project.

### Maven usage

To use in our project you can access the latest binaries from maven central:


{{< highlight xml>}}
<dependency>
   <artifactId>ratis-server</artifactId>
   <groupId>org.apache.ratis</groupId>
</dependency>
{{< /highlight >}}


You also need to include *one* of the transports:

{{< highlight xml>}}
<dependency>
   <artifactId>ratis-grpc</artifactId>
   <groupId>org.apache.ratis</groupId>
</dependency>
{{< /highlight >}}

{{< highlight xml>}}
 <dependency>
   <artifactId>ratis-netty</artifactId>
   <groupId>org.apache.ratis</groupId>
</dependency>
{{< /highlight >}}

{{< highlight xml>}}
<dependency>
   <artifactId>ratis-hadoop</artifactId>
   <groupId>org.apache.ratis</groupId>
</dependency>
{{< /highlight >}}

Please note that Apache Hadoop dependencies are shaded, so it's safe to use hadoop transport with different versions of Hadoop.

