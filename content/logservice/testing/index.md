---
title: LogService Testing
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

The LogService presently has two means for testing itself: Docker with Compose
orchestration and VirtualBox with Vagrant orchestration.

Docker is suitable for a quick and lightweight orchestration of a full LogService
installation. Vagrant, while heavier-weight that the Docker automation, has the added benefit of being able to leverage [Namazu](http://osrg.github.io/namazu/) for failure
scenarios. Please find more on each using the below references.

* [Docker]({{< ref "docker.md" >}})
* [Vagrant]({{< ref "vagrant.md" >}})
