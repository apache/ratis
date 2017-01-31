We shade protobuf and other libraries such as Netty, gRPC and Hadoop
so that applications using Raft may use protobuf and other libraries with versions different 
from the versions used here.

The library requires the shaded sources for compilation. To generate them,
run the following command under `raft-proto-shaded/`

- `mvn package -Dcompile-protobuf -DskipTests`

Then run your normal mvn commands to build the whole library.

