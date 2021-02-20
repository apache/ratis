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

# Apache Ratis

This is the binary distribution of Apache Ratis.

Apache Ratis is a java library that implements the RAFT protocol.

First of all: it's a library. To use it use java dependency management tool (such as maven and gradle).
The required artifacts are available from the maven central or the apache nexus under the org.apache.ratis groupId.

This distribution also includes some example raft server in the examples directory which includes an example
implementation of the key raft elements: raftlog and state machine. The are for demonstration purposes only.


For more deails see:

https://ratis.apache.org
