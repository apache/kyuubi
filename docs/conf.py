#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#

import os
import sys
import shlex
import subprocess
import datetime

sys.path.insert(0, os.path.abspath('.'))

source_suffix = {
    '.rst': 'restructuredtext',
    '.txt': 'restructuredtext',
    '.md': 'markdown',
}

# -- Project information -----------------------------------------------------

project = 'Kyuubi'

year = datetime.datetime.now().strftime("%Y")

copyright = year + ' The Apache Software Foundation, Licensed under the Apache License, Version 2.0'


author = 'Apache Kyuubi Community'

# The full version, including alpha/beta/rc tags
release = subprocess.getoutput("grep 'kyuubi-parent' -C1 ../pom.xml | grep '<version>' | awk -F '[<>]' '{print $3}'")


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.mathjax',
    'sphinx_togglebutton',
    'myst_parser',
    'notfound.extension',
]

myst_enable_extensions = [
    "amsmath",
    "attrs_inline",
    "colon_fence",
    "deflist",
    "dollarmath",
    "fieldlist",
    "html_admonition",
    "html_image",
    "replacements",
    "smartquotes",
    "strikethrough",
    "substitution",
    "tasklist",
]

myst_substitutions = {
  "release": release,
  "kyuubi_hive_jdbc_shaded_maven_ref":
"""
```xml
<dependency>
    <groupId>org.apache.kyuubi</groupId>
    <artifactId>kyuubi-hive-jdbc-shaded</artifactId>
    <version>%s</version>
</dependency>
```
""" % release,
}

root_doc = 'index'

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_book_theme'
html_theme_options = {
    "repository_url": "https://github.com/apache/kyuubi",
    "use_repository_button": True,
    "use_edit_page_button": True,
    "use_download_button": True,
    "use_fullscreen_button": True,
    "repository_branch": "master",
    "path_to_docs": "docs",
    "logo_only": True,
    "home_page_in_toc": False,
    "show_navbar_depth": 1,
    "show_toc_level": 2,
    "announcement": "&#129418; Welcome to Kyuubi’s online documentation &#x2728;, v" + release,
    "toc_title": "",
    "extra_navbar": "Version " + release,
}

html_logo = 'imgs/logo.png'
html_favicon = 'imgs/logo_red_short.png'
html_title = 'Apache Kyuubi'

pygments_style = 'sphinx'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
html_css_files = ["css/custom.css"]
