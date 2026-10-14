<!--
- Licensed to the Apache Software Foundation (ASF) under one or more
- contributor license agreements.  See the NOTICE file distributed with
- this work for additional information regarding copyright ownership.
- The ASF licenses this file to You under the Apache License, Version 2.0
- (the "License"); you may not use this file except in compliance with
- the License.  You may obtain a copy of the License at
-
-   http://www.apache.org/licenses/LICENSE-2.0
-
- Unless required by applicable law or agreed to in writing, software
- distributed under the License is distributed on an "AS IS" BASIS,
- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
- See the License for the specific language governing permissions and
- limitations under the License.
-->

# Documentation Style Guide

This guide contains guidelines, not rules. While guidelines are important
to follow, they are not hard and fast rules. It's important to use your
own judgement and discretion when creating content, and to depart from the
guidelines when necessary to improve the quality and effectiveness of your
content. Ultimately, the goal is to create content that is clear, concise,
and useful to your audience, and sometimes deviating from the guidelines
may be necessary to achieve that goal.

## Goals

- Source text files are readable and portable
- Source diagram files are editable
- Source files are maintainable over time and across community

## License Header

All original documents should include the ASF license header. All reproduced
or quoted content should be authorized and attributed to the source.

If you are about to quote some from commercial materials, please refer to
[ASF 3RD PARTY LICENSE POLICY](https://www.apache.org/legal/resolved.html#asf-3rd-party-license-policy), or consult the Apache Kyuubi PMC to avoid
legality issues.

## General Style

- Use [Markdown](https://en.wikipedia.org/wiki/Markdown) format with [MyST](https://myst-parser.readthedocs.io/en/latest) flavor if needed, avoid HTML hacks.
- Use [draw.io](https://www.diagrams.net/) for drawing or editing an image, and export it as PNG for referencing in document.
  A pull request should commit both of them Use Kyuubi for short instead of Apache Kyuubi after the first time in the same page.
- Character line limit: 78, except unbreakable ones.
- Prefer lists to tables.
- Prefer unordered list than ordered.

## Markdown

### Headings

- Use **Pascal Case**, every word starts with an uppercase letter, e.g., 'Documentation Style Guide'
- Use a max of **three levels**
  - Split into multiple files when there comes an H4
- **DO NOT** use numbering for sections
- **DO NOT** use "Kyuubi" in titles if possible

## Images

Use images only when they provide helpful visual explanations of information
otherwise difficult to express with words

## Third-party references

If the preceding references don't provide explicit guidance, then see these
third-party references, depending on the nature of your question:

- [Google developer documentation style](https://developers.google.com/style)
- [Apple Style Guide](https://help.apple.com/applestyleguide/)
- [Red Hat supplementary style guide for product documentation](https://redhat-documentation.github.io/supplementary-style-guide/)

