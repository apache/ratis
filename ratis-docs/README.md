<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# Apache Ratis docs

This subproject contains the inline documentation for Apache Ratis.

## View rendered documents
To view the documents locally, you can run:

```
cd ratis-docs
mvn site:run
```

Then visit http://localhost:8080/ to view rendered documents.

## Write document
To create new document, please add markdown files into `src/site/markdown` folder, and then create a link in `site.xml`. For example, `site/markdown/cli.md` could be accessed by `cli.html`.

For more about the usage, please refer to the [documentation of maven-site-plugin](https://maven.apache.org/guides/mini/guide-site.html).

