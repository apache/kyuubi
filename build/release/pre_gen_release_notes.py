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

# This script simplifies the process of creating release notes, it
# - folds the original and the revert commits
# - filters out unrelated commits
# - generates the contributor list
# - canonicalizes the contributors' name with the known_translations

# TODO
# - canonicalizes the commits' title

# Usage:
#   set environment variables: RELEASE_TAG and PREVIOUS_RELEASE_TAG, then perform
#   ./pre_gen_release_notes.py
# Example:
#   RELEASE_TAG=v1.8.1 PREVIOUS_RELEASE_TAG=v1.8.0 ./pre_gen_release_notes.py

# It outputs
# - commits-${RELEASE_TAG}.txt:      the canonical commit list
# - contributors-${RELEASE_TAG}.txt: the canonical contributor list

import os
import re
import sys

from release_utils import (
    tag_exists,
    get_commits,
    yes_or_no_prompt,
    get_date,
    is_valid_author,
    capitalize_author,
    print_indented
)

RELEASE_TAG = os.environ.get("RELEASE_TAG")
if RELEASE_TAG is None:
    sys.exit("RELEASE_TAG is required")
if not tag_exists(RELEASE_TAG):
    sys.exit("RELEASE_TAG: %s does not exist!" % RELEASE_TAG)

PREVIOUS_RELEASE_TAG = os.environ.get("PREVIOUS_RELEASE_TAG")
if PREVIOUS_RELEASE_TAG is None:
    sys.exit("PREVIOUS_RELEASE_TAG is required")
if not tag_exists(PREVIOUS_RELEASE_TAG):
    sys.exit("PREVIOUS_RELEASE_TAG: %s does not exist!" % PREVIOUS_RELEASE_TAG)

release_dir = os.path.dirname(os.path.abspath(__file__))
commits_file_name = "commits-%s.txt" % RELEASE_TAG
contributors_file_name = "contributors-%s.txt" % RELEASE_TAG

# Gather commits found in the new tag but not in the old tag.
# This filters commits based on both the git hash and the PR number.
# If either is present in the old tag, then we ignore the commit.
print("Gathering new commits between tags %s and %s" % (PREVIOUS_RELEASE_TAG, RELEASE_TAG))
release_commits = get_commits(RELEASE_TAG)
previous_release_commits = get_commits(PREVIOUS_RELEASE_TAG)
previous_release_hashes = set()
previous_release_prs = set()
for old_commit in previous_release_commits:
    previous_release_hashes.add(old_commit.get_hash())
    if old_commit.get_pr_number():
        previous_release_prs.add(old_commit.get_pr_number())
new_commits = []
for this_commit in release_commits:
    this_hash = this_commit.get_hash()
    this_pr_number = this_commit.get_pr_number()
    if this_hash in previous_release_hashes:
        continue
    if this_pr_number and this_pr_number in previous_release_prs:
        continue
    new_commits.append(this_commit)
if not new_commits:
    sys.exit("There are no new commits between %s and %s!" % (PREVIOUS_RELEASE_TAG, RELEASE_TAG))

# Prompt the user for confirmation that the commit range is correct
print("\n==================================================================================")
print("Release tag: %s" % RELEASE_TAG)
print("Previous release tag: %s" % PREVIOUS_RELEASE_TAG)
print("Number of commits in this range: %s" % len(new_commits))
print("")

if yes_or_no_prompt("Show all commits?"):
    print_indented(new_commits)
print("==================================================================================\n")
if not yes_or_no_prompt("Does this look correct?"):
    sys.exit("Ok, exiting")

# Filter out special commits
releases = []
reverts = []
no_tickets = []
effective_commits = []

def is_release(commit_title):
    return "[release]" in commit_title.lower()


def has_no_ticket(commit_title):
    return not re.findall("\\[KYUUBI\\s\\#[0-9]+\\]", commit_title.upper())


def is_revert(commit_title):
    return "revert" in commit_title.lower()


for c in new_commits:
    t = c.get_title()
    if not t:
        continue
    elif is_release(t):
        releases.append(c)
    elif is_revert(t):
        reverts.append(c)
    elif has_no_ticket(t):
        no_tickets.append(c)
    else:
        effective_commits.append(c)


# Warn against ignored commits
if releases or reverts or no_tickets:
    print("\n==================================================================================")
    if releases:
        print("Found %d release commits" % len(releases))
    if reverts:
        print("Found %d revert commits" % len(reverts))
    if no_tickets:
        print("Found %d commits with no Ticket" % len(no_tickets))
    print("==================== Warning: these commits will be ignored ======================\n")
    if releases:
        print("Release (%d)" % len(releases))
        print_indented(releases)
    if reverts:
        print("Revert (%d)" % len(reverts))
        print_indented(reverts)
    if no_tickets:
        print("No Ticket (%d)" % len(no_tickets))
        print_indented(no_tickets)
    print("==================== Warning: the above commits will be ignored ==================\n")
prompt_msg = "%d effective commits left to process after filtering. OK to proceed?" % len(effective_commits)
if not yes_or_no_prompt(prompt_msg):
    sys.exit("OK, exiting.")


# Load known author translations that are cached locally
known_translations = {}
known_translations_file_name = "known_translations"
known_translations_file = open(os.path.join(release_dir, known_translations_file_name), "r")
for line in known_translations_file:
    if line.startswith("#") or not line.strip():
        continue
    [old_name, new_name] = line.strip("\n").split(" - ")
    known_translations[old_name] = new_name
known_translations_file.close()

# Keep track of warnings to tell the user at the end
warnings = []

# The author name that needs to translate
invalid_authors = set()
authors = set()
print("\n=========================== Compiling contributor list ===========================")
for commit in effective_commits:
    _hash = commit.get_hash()
    title = commit.get_title()
    issues = re.findall("\\[KYUUBI\\s\\#[0-9]+\\]", title.upper())
    author = commit.get_author()
    date = get_date(_hash)
    # Translate the known author name
    if author in known_translations:
        author = known_translations[author]
    elif is_valid_author(author):
        # If the author name is invalid, keep track of it along
        # with all associated issues so we can translate it later
        author = capitalize_author(author)
    else:
        invalid_authors.add(author)
    authors.add(author)
    print("  Processed commit %s authored by %s on %s" % (_hash, author, date))
print("==================================================================================\n")

commits_file = open(os.path.join(release_dir, commits_file_name), "w")
for commit in effective_commits:
    if commit.get_hash() not in map(lambda revert: revert.get_revert_hash(), reverts):
        commits_file.write(commit.title + "\n")
for commit in no_tickets:
    commits_file.write(commit.title + "\n")
commits_file.close()
print("Commits list is successfully written to %s!" % commits_file_name)

# Write to contributors file ordered by author names
# Each line takes the format " * Author Name"
# e.g. * Cheng Pan
# e.g. * Fu Chen
contributors_file = open(os.path.join(release_dir, contributors_file_name), "w")
sorted_authors = list(authors)
sorted_authors.sort(key=lambda author: author.split(" ")[0].lower())
for author in sorted_authors:
    contributors_file.write("* %s\n" % author)
contributors_file.close()
print("Contributors list is successfully written to %s!" % contributors_file_name)

# Prompt the user to translate author names if necessary
if invalid_authors:
    warnings.append("Found the following invalid authors:")
    for a in invalid_authors:
        warnings.append("\t%s" % a)
    warnings.append("Please update 'known_translations'.")

# Log any warnings encountered in the process
if warnings:
    print("\n============ Warnings encountered while creating the contributor list ============")
    for w in warnings:
        print(w)
    print("Please correct these in the final contributors list at %s." % contributors_file_name)
    print("==================================================================================\n")
