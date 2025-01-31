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

# Apache Ratis web page

This is the source code of the website of Apache Ratis.

## Hugo Installation

To render it you need hugo static site generator (https://gohugo.io/getting-started/installing) which is available for the most popular platforms as a single binary.

On OSX, this can be installed via HomeBrew: `brew install hugo`. For other operating system, please refer to the
aforementioned Hugo documentation for installation steps.

## Building

To render the final website, use the provided `build.sh` script. 
```
./build.sh <website_output>
```
It will perform a license check on the source files
and then generate the website in the given output directory

To iteratively develop the website, you can use the `serve` command to start a local webserver with your content changes
rendered in realtime:

```
hugo server -c <website_output>
```

## Publishing website changes

Committers must ensure that the state of the `asf-site-source` and `asf-site` branches are in sync at all times.
Committers must never manually edit content in the `asf-site` branch.

Content pushed to the `asf-site` branch is automatically published to the public website:
https://ratis.apache.org

There is (presently) no automation to automatically keep these branches in sync, but a general guide is to do the following.
These steps use two checkouts of the Git repo, one for `asf-site` and another for `asf-site-source`. Beware that these steps
are destructive to any local modifications:

First time only!
```bash
$ git clone https://github.com/apache/ratis ratis-site.git
$ cp -r ratis-site.git ratis-site-source.git
$ pushd ratis-site.git && git checkout -t origin/asf-site && popd
$ pushd ratis-site-source.git && git checkout -t origin/asf-site-source && popd
```

To modify the website:
```bash
$ pushd ratis-site.git && git pull && popd
$ pushd ratis-site-source.git && git pull
$ # hack hack hack
$ ./build.sh ../ratis-site.git
```
