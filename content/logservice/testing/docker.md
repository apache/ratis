---
title: Docker Testing
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

The Docker orchestration is comprised of the following:

1. A Docker image which has the necessary software to run the LogService
2. A docker-compose.yml file which can launch the necessary containers for a full-fledged LogService "cluster"
3. Scripts to build the Docker image and launch a client environment to interact
with a running cluster via Compose.

## Building the Docker image

```bash
$ mvn clean package assembly:single -DskipTests
$ cd ratis-logservice && ./build-docker.sh
```

The above will create a Docker image tagged as `ratis-logservice:latest`.

## Launching a cluster via Compose

```bash
$ docker-compose up -d
```

The Compose orchestration will launch three MetadataServer containers and three
Worker containers, all on the same Docker network. The `-d` option detaches the
container logs from your current shell.

## Connecting a client

```bash
$ ./client-env.sh
$ ./bin/shell <...>
$ ./bin/load-test <...>
```

The `client-env.sh` script will launch a Docker container which is on the same
network as our cluster running in Compose.

## Debugging the cluster

Use `docker logs` to inspect the output from a specific container. You must pass
the name of the container (obtained via `docker-compose ps` or `docker ps`) to
`docker logs`.

You can also "attach" to a container via `docker exec` to inspect the environment
in which the process is running. Again, using the name of a container obtained as
described above, use `docker exec -it <name> /bin/sh` to attach to the container.
