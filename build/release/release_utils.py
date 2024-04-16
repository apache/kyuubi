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

# This script is inspired by Apache Spark

# This file contains helper methods used in creating a release.

import re
import sys
from subprocess import Popen, PIPE


# Prompt the user to answer yes or no until they do so
def yes_or_no_prompt(msg):
    response = input("%s [y/n]: " % msg)
    while response != "y" and response != "n":
        return yes_or_no_prompt(msg)
    return response == "y"


def run_cmd(cmd):
    return Popen(cmd, stdout=PIPE).communicate()[0].decode("utf8")


def run_cmd_error(cmd):
    return Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()[1].decode("utf8")


def get_date(commit_hash):
    return run_cmd(["git", "show", "--quiet", "--pretty=format:%cd", commit_hash])


def tag_exists(tag):
    stderr = run_cmd_error(["git", "show", tag])
    return "error" not in stderr and "fatal" not in stderr


# A type-safe representation of a commit
class Commit:
    def __init__(self, _hash, author, title, pr_number=None, revert_hash=None):
        self._hash = _hash
        self.author = author
        self.title = title
        self.pr_number = pr_number
        self.revert_hash = revert_hash

    def get_hash(self):
        return self._hash

    def get_author(self):
        return self.author

    def get_title(self):
        return self.title

    def get_pr_number(self):
        return self.pr_number

    def get_revert_hash(self):
        return self.revert_hash

    def __str__(self):
        closes_pr = "(Closes #%s)" % self.pr_number if self.pr_number else ""
        revert_commit = "(Reverts %s)" % self.revert_hash if self.revert_hash else ""
        return "%s %s %s %s %s" % (self._hash, self.author, self.title, closes_pr, revert_commit)


# Return all commits that belong to the specified tag.
#
# Under the hood, this runs a `git log` on that tag and parses the fields
# from the command output to construct a list of Commit objects. Note that
# because certain fields reside in the commit description, we need to do
# some intelligent regex parsing to extract those fields.
def get_commits(tag):
    commit_start_marker = "|=== COMMIT START MARKER ===|"
    commit_end_marker = "|=== COMMIT END MARKER ===|"
    field_end_marker = "|=== COMMIT FIELD END MARKER ===|"
    log_format = (
            commit_start_marker
            + "%h"
            + field_end_marker
            + "%an"
            + field_end_marker
            + "%s"
            + commit_end_marker
            + "%b"
    )
    output = run_cmd(["git", "log", "--quiet", "--pretty=format:" + log_format, tag])
    commits = []
    raw_commits = [c for c in output.split(commit_start_marker) if c]
    for commit in raw_commits:
        if commit.count(commit_end_marker) != 1:
            print("Commit end marker not found in commit: ")
            for line in commit.split("\n"):
                print(line)
            sys.exit(1)
        # Separate commit digest from the body
        # From the digest we extract the hash, author and the title
        # From the body, we extract the PR number and the github username
        [commit_digest, commit_body] = commit.split(commit_end_marker)
        if commit_digest.count(field_end_marker) != 2:
            sys.exit("Unexpected format in commit: %s" % commit_digest)
        [_hash, author, title] = commit_digest.split(field_end_marker)
        # The PR number and github username is in the commit message
        # itself and cannot be accessed through any GitHub API
        pr_number = None
        match = re.search("Closes #([0-9]+) from ([^/\\s]+)/", commit_body)
        if match:
            [pr_number, github_username] = match.groups()
            # If the author name is not valid, use the github
            # username so we can translate it properly later
            if not is_valid_author(author):
                author = github_username
        author = author.strip()
        revert_hash = None
        match = re.search("This reverts commit ([0-9a-f]+)", commit_body)
        if match:
            [revert_hash] = match.groups()
            revert_hash = revert_hash[:9]
        commit = Commit(_hash, author, title, pr_number, revert_hash)
        commits.append(commit)
    return commits


# Return whether the given name is in the form <First Name><space><Last Name>
def is_valid_author(author):
    if not author:
        return False
    return " " in author and not re.findall("[0-9]", author)


# Capitalize the first letter of each word in the given author name
def capitalize_author(author):
    if not author:
        return None
    words = author.split(" ")
    words = [w[0].capitalize() + w[1:] for w in words if w]
    return " ".join(words)


def print_indented(_list):
    for x in _list:
        print("  %s" % x)
