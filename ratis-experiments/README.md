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

# Apache Ratis Experiments

This module has been created to house experimental projects.
Details for running the experiment are mentioned below. 

### Building and Running The Experiments

#### Requirements
The flatbuffers compiler(flatc) is required for building this module. 
You can refer to this [link](https://rwinslow.com/posts/how-to-install-flatbuffers/) for instructions on how to build from flatbuffers source.

#### Build Commands
The repository can be complied using `mvn clean package -DskipTests -DbuildExp` under the project root directory;
see also [BUILDING.md](../BUILDING.md).


## Zero-copy semantics - Flatbuffers

This program was developed to test the efficiency of flatbuffers vs protobuffers in terms of intermediate buffer copying.
The Client creates a 1MB buffer, fills it with data and transfers it over GRPC.

### Run Datatransfer Server and Client
There two pairs of Server and Client: (ClientProto, ServerProto) and (ClientFlat,ServerFlat).

run the client and servers by these commands from ratis-experiments directory:

for server: `java -cp target/ratis-experiments-1.1.0-SNAPSHOT.jar org.apache.ratis.experiments.flatbuffers.server.ServerFlat`

for client: `java -cp target/ratis-experiments-1.1.0-SNAPSHOT.jar org.apache.ratis.experiments.flatbuffers.client.ClientFlat {numberOfReps}`

replace noOfReps with the number of times you want to transfer data(defaults to 100,000).

### Findings:
Current releases of flatbuffers with GRPC, do not provide zero-copy semantics in Java and have similar performance characteristics as compared to protobufs.
Clarified in [flatbuffers issue #6023](https://github.com/google/flatbuffers/issues/6023).

## Zero-copy semantics - Netty

The program was developed to test ability of Netty to avoid intermediate buffer copies created while serialization/deserialization. 
The Client creates a 1MB buffer, fills it with data and transfers it 10,000 times to a NettyServer instance over TCP.

### Running Server and Client
The code for each(Client and Server) can be found in NettyZeroCopy submodule. Each of the necessary classes have been provided under encoders, decoders and objects.

Run the client and servers by these commands from ratis-experiments directory:

for server: `java -cp target/ratis-experiments-1.1.0-SNAPSHOT.jar org.apache.ratis.experiments.nettyzerocopy.server.NettyServer`

for client: `java -cp target/ratis-experiments-1.1.0-SNAPSHOT.jar org.apache.ratis.experiments.nettyzerocopy.client.NettyClient`

### Findings:
Zero-copy semantics were achieved using Netty with significant performance improvements over GRPC. 