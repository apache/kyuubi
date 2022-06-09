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

import sys

def usage():
  print('Usage: %s <NOTICE file> <APPEND file1> <APPEND file2> ...' % sys.argv[0])

redundant_text_list = [
"""This product includes software developed at
The Apache Software Foundation (http://www.apache.org/).""",
"""This product includes software developed at
The Apache Software Foundation (https://www.apache.org/).""",
"""This product includes software developed by
The Apache Software Foundation (http://www.apache.org/)."""
]

def append(notice_file, append_file):
    with open(notice_file, 'r') as f:
        notice_text = f.read()

    with open(append_file, 'r') as f:
        append_text = f.read()

    for text in redundant_text_list:
        append_text = append_text.replace(text, '')

    append_text = append_text.strip()

    if not append_text in notice_text:
        with open(notice_file, 'a') as f:
            f.write(append_text)
            f.write('\n\n')

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(sys.argv)
        usage()
        sys.exit(-1)

    notice_file = sys.argv[1]
    for append_file in sys.argv[2:]:
      append(notice_file, append_file)
