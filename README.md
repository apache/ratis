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

To render the final website, use the provided `build.sh` script. This script will generate the website in the directory
`public/` and also perform a license check on the source files (prior to commit).

```
hugo
```

To iteratively develop the website, you can use the `serve` command to start a local webserver with your content changes
rendered in realtime:

```
hugo serve
```

## Publishing website changes

Committers must ensure that the state of the `asf-site-source` and `asf-site` branches are in sync at all times.
Committers must never make changes directly to the `asf-site` branch.

Content pushed to the `asf-site` branch is automatically published to the public website:
https://ratis.incubator.apache.org

There is no automation to automatically keep these branches in sync, but a general guide is to do the following. Beware
that these steps are destructive!

```bash
$ git checkout asf-site-source && //hack hack hack
$ ./build.sh
$ mv public ../
$ git checkout asf-site
$ rm -i ./*
* cp ../public/*
$ rm -i -r categories post tags
$ cp -r ../public/categories ../public/post ../public/tags .
```
