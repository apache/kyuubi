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

# Utility for creating well-formed pull request merges and pushing them to Kyuubi
# For committers:
# Please check your local git envs via `git remote -v` which should
# apache	git@github.com:apache/kyuubi.git (fetch)
# apache	git@github.com:apache/kyuubi.git (push)
# origin	git@github.com:[ YOUR GITHUB USER NAME ]/kyuubi.git (fetch)
# origin	git@github.com:[ YOUR GITHUB USER NAME ]/kyuubi.git (push)

import json
import os
import re
import subprocess
import sys
from urllib.error import HTTPError
from urllib.request import Request
from urllib.request import urlopen

KYUUBI_HOME = os.environ.get("KYUUBI_HOME", os.getcwd())
PR_REMOTE_NAME = os.environ.get("PR_REMOTE_NAME", "apache")
PUSH_REMOTE_NAME = os.environ.get("PUSH_REMOTE_NAME", "apache")
GITHUB_OAUTH_KEY = os.environ.get("GITHUB_OAUTH_KEY")
GITHUB_API_BASE = "https://api.github.com/repos/apache/kyuubi"
GITHUB_COMMIT_BASE = "https://github.com/apache/kyuubi/commit"
BRANCH_PREFIX = "PR_TOOL"
_MERGE_CLOSING_RE = re.compile(r"^Closes #(\d+) from \S+\s*$", re.MULTILINE)
_MERGE_AUTHORS_RE = re.compile(r"^(?:Lead-authored-by|Authored-by):", re.MULTILINE)


def get_json(url):
    try:
        request = Request(url)
        if GITHUB_OAUTH_KEY:
            request.add_header("Authorization", "token %s" % GITHUB_OAUTH_KEY)
        return json.load(urlopen(request))
    except HTTPError as e:
        if (
            "X-RateLimit-Remaining" in e.headers
            and e.headers["X-RateLimit-Remaining"] == "0"
        ):
            print(
                "Exceeded the GitHub API rate limit; see the instructions in "
                + "dev/merge_kyuubi_pr.py to configure an OAuth token for making authenticated "
                + "GitHub requests."
            )
        else:
            print("Unable to fetch URL, exiting: %s" % url, e)
        sys.exit(-1)


def fail(msg):
    print(msg)
    clean_up()
    sys.exit(-1)


def run_cmd(cmd):
    print(cmd)
    if isinstance(cmd, list):
        return subprocess.check_output(cmd).decode("utf-8")
    else:
        return subprocess.check_output(cmd.split(" ")).decode("utf-8")


def get_input(prompt, options):
    while True:
        answer = input(prompt).strip()
        if isinstance(options, str):
            if re.fullmatch(options, answer):
                return answer
        else:
            normalized_answer = answer.lower()
            if normalized_answer in options:
                return normalized_answer
        print("Invalid input. Please try again.")


def continue_maybe(prompt):
    result = get_input("\n%s (y/N): " % prompt, ["y", "n", ""]).lower()
    if result != "y":
        fail("Okay, exiting")


def clean_up():
    if "original_head" in globals():
        print("Restoring head pointer to %s" % original_head)
        run_cmd("git checkout %s" % original_head)

        branches = run_cmd("git branch").replace(" ", "").split("\n")

        for branch in list(filter(lambda x: x.startswith(BRANCH_PREFIX), branches)):
            print("Deleting local branch %s" % branch)
            run_cmd("git branch -D %s" % branch)


def comment_pr(pr_num, body):
    url = "%s/issues/%s/comments" % (GITHUB_API_BASE, pr_num)
    data = json.dumps({"body": body}).encode("utf-8")
    request = Request(url, data=data, method="POST")
    request.add_header("Content-Type", "application/json")
    request.add_header("Accept", "application/vnd.github+json")
    if GITHUB_OAUTH_KEY:
        request.add_header("Authorization", "token %s" % GITHUB_OAUTH_KEY)
    try:
        return json.load(urlopen(request))
    except HTTPError as e:
        print("Failed to comment on PR #%s: HTTP %s %s" % (pr_num, e.code, e.reason))
        return None


def post_merge_comment(pr_num, merged_commits):
    """Post a comment recording every branch the change landed on."""
    if not merged_commits:
        return

    lines = [
        "- merged into %s %s/%s" % (ref, GITHUB_COMMIT_BASE, commit_hash)
        for ref, commit_hash in merged_commits
    ]
    summary = "**Merge Summary:**\n" + "\n".join(lines)
    attribution = "*Posted by `merge_kyuubi_pr.py`*"
    body = "%s\n\n%s" % (summary, attribution)
    print(
        "\nPosting merge comment on PR #%s:\n\n%s\n%s" % (pr_num, summary, attribution)
    )
    if not GITHUB_OAUTH_KEY:
        print("GITHUB_OAUTH_KEY is not set; skipping the merge comment.")
        return
    comment_pr(pr_num, body)


def close_pr(pr_num):
    url = "%s/pulls/%s" % (GITHUB_API_BASE, pr_num)
    data = json.dumps({"state": "closed"}).encode("utf-8")
    request = Request(url, data=data, method="PATCH")
    request.add_header("Content-Type", "application/json")
    request.add_header("Accept", "application/vnd.github+json")
    if GITHUB_OAUTH_KEY:
        request.add_header("Authorization", "token %s" % GITHUB_OAUTH_KEY)
    try:
        return json.load(urlopen(request))
    except HTTPError as e:
        print("Failed to close PR #%s: HTTP %s %s" % (pr_num, e.code, e.reason))
        return None


def default_pick_branch(branch_names, already_picked):
    """Return the newest release branch that has not received the change.

    >>> default_pick_branch(["branch-1.12", "branch-1.11"], ("master",))
    'branch-1.12'
    >>> default_pick_branch(["branch-1.12", "branch-1.11"], ("master", "branch-1.12"))
    'branch-1.11'
    >>> default_pick_branch(["branch-1.12"], ("master", "branch-1.12")) is None
    True
    """
    remaining = [branch for branch in branch_names if branch not in already_picked]
    return remaining[0] if remaining else None


def merge_footer_pr(message):
    """Return the PR number in the final generated merge footer.

    >>> footer = "Closes #1 from a/b.\\n\\nAuthored-by: A <a@example.org>"
    >>> merge_footer_pr("Title\\n\\n" + footer)
    1
    >>> footer = (
    ...     "Closes #1 from a/b.\\n\\nCloses #2\\n\\n"
    ...     "abcdef [Author] Title\\n\\nAuthored-by: A <a@example.org>"
    ... )
    >>> merge_footer_pr("Title\\n\\n" + footer)
    1
    >>> merge_footer_pr("Title\\n\\nNo footer") is None
    True
    """
    authors = list(_MERGE_AUTHORS_RE.finditer(message))
    if not authors:
        return None
    closings = list(_MERGE_CLOSING_RE.finditer(message, 0, authors[-1].start()))
    return int(closings[-1].group(1)) if closings else None


def has_merge_footer(message, pr_num):
    """Whether the final generated merge footer closes pr_num.

    >>> footer = "Closes #1 from a/b.\\n\\nAuthored-by: A <a@example.org>"
    >>> has_merge_footer("Title\\n\\n" + footer, 1)
    True
    >>> has_merge_footer("Title\\n\\n" + footer, 2)
    False
    """
    return merge_footer_pr(message) == int(pr_num)


def merge_commit_candidates(pr_events):
    """Split merge events into closed and referenced commits, oldest first.

    >>> merge_commit_candidates([
    ...     {"event": "closed", "commit_id": "a", "created_at": "2"},
    ...     {"event": "referenced", "commit_id": "b", "created_at": "1"},
    ... ])
    (['a'], ['b'])
    >>> merge_commit_candidates([{"event": "closed", "commit_id": None}])
    ([], [])
    """

    def commits_of(event_name):
        matched = [
            event
            for event in pr_events
            if event["event"] == event_name and event["commit_id"] is not None
        ]
        return [
            event["commit_id"]
            for event in sorted(matched, key=lambda event: event["created_at"])
        ]

    return commits_of("closed"), commits_of("referenced")


def find_merge_commit(pr_num, pr_events):
    """Return the latest commit that merged pr_num, or None."""

    def message_of(commit_hash):
        return get_json("%s/commits/%s" % (GITHUB_API_BASE, commit_hash))["commit"][
            "message"
        ]

    closed_commits, referenced_commits = merge_commit_candidates(pr_events)
    if closed_commits:
        return closed_commits[-1], message_of(closed_commits[-1])

    for commit_hash in reversed(referenced_commits):
        message = message_of(commit_hash)
        if has_merge_footer(message, pr_num):
            return commit_hash, message
    return None, None


def fix_title(text, num):
    if re.search(r"^\[KYUUBI\s#[0-9]{3,6}\].*", text):
        return text

    return "[KYUUBI #%s] %s" % (num, text)


# merge the requested PR and return the merge hash
def merge_pr(pr_num, target_ref, title, body, pr_repo_desc, pr_author, co_authors):
    pr_branch_name = "%s_MERGE_PR_%s" % (BRANCH_PREFIX, pr_num)
    target_branch_name = "%s_MERGE_PR_%s_%s" % (
        BRANCH_PREFIX,
        pr_num,
        target_ref.upper(),
    )
    run_cmd("git fetch %s pull/%s/head:%s" % (PR_REMOTE_NAME, pr_num, pr_branch_name))
    run_cmd("git fetch %s %s:%s" % (PUSH_REMOTE_NAME, target_ref, target_branch_name))
    run_cmd("git checkout %s" % target_branch_name)

    had_conflicts = False
    try:
        run_cmd(["git", "merge", pr_branch_name, "--squash"])
    except Exception as e:
        msg = "Error merging: %s\nWould you like to manually fix-up this merge?" % e
        continue_maybe(msg)
        msg = "Okay, please fix any conflicts and 'git add' conflicting files... Finished?"
        continue_maybe(msg)
        had_conflicts = True

    primary_author = input(
        'Enter primary author in the format of "name <email>" [%s]: ' % pr_author
    )
    if primary_author == "":
        primary_author = pr_author

    commits = run_cmd(
        ["git", "log", "HEAD..%s" % pr_branch_name, "--pretty=format:%h [%an] %s"]
    ).split("\n\n")

    merge_message_flags = []

    merge_message_flags += ["-m", title]
    if body is not None:
        # We remove @ symbols from the body to avoid triggering e-mails
        # to people every time someone creates a public fork of Kyuubi.
        merge_message_flags += ["-m", body.replace("@", "")]

    committer_name = run_cmd("git config --get user.name").strip()
    committer_email = run_cmd("git config --get user.email").strip()

    if had_conflicts:
        message = (
            "This patch had conflicts when merged, resolved by\nCommitter: %s <%s>"
            % (committer_name, committer_email)
        )
        merge_message_flags += ["-m", message]

    # The string "Closes #%s" string is required for GitHub to correctly close the PR
    merge_message_flags += ["-m", "Closes #%s from %s." % (pr_num, pr_repo_desc)]

    for issueId in re.findall("KYUUBI #[0-9]{3,5}", title):
        merge_message_flags += ["-m", issueId.replace("KYUUBI", "Closes")]

    for c in commits:
        merge_message_flags += ["-m", c]

    authors = "Authored-by:" if len(co_authors) == 0 else "Lead-authored-by:"
    authors += " %s" % primary_author
    if len(co_authors) > 0:
        authors += "\n" + "\n".join(
            ["Co-authored-by: %s" % co_author for co_author in co_authors]
        )
    authors += "\n" + "Signed-off-by: %s <%s>" % (committer_name, committer_email)

    merge_message_flags += ["-m", authors]

    run_cmd(["git", "commit", '--author="%s"' % primary_author] + merge_message_flags)

    continue_maybe(
        "Merge complete (local ref %s). Push to %s?"
        % (target_branch_name, PUSH_REMOTE_NAME)
    )

    try:
        run_cmd(
            "git push %s %s:%s" % (PUSH_REMOTE_NAME, target_branch_name, target_ref)
        )
    except Exception as e:
        clean_up()
        fail("Exception while pushing: %s" % e)

    merge_hash = run_cmd("git rev-parse %s" % target_branch_name).strip()
    clean_up()
    print("Pull request #%s merged!" % pr_num)
    print("Merge hash: %s" % merge_hash)
    return merge_hash


def cherry_pick(pr_num, merge_hash, default_branch, branch_names):
    while True:
        pick_ref = input("Enter a branch name [%s]: " % default_branch)
        if pick_ref == "":
            pick_ref = default_branch
        if pick_ref in branch_names:
            break
        print(
            "'%s' is not a known release branch. Valid branches: %s. Please try again."
            % (pick_ref, ", ".join(branch_names))
        )

    pick_branch_name = "%s_PICK_PR_%s_%s" % (BRANCH_PREFIX, pr_num, pick_ref.upper())

    run_cmd("git fetch %s %s:%s" % (PUSH_REMOTE_NAME, pick_ref, pick_branch_name))
    run_cmd("git checkout %s" % pick_branch_name)

    try:
        run_cmd("git cherry-pick -sx %s" % merge_hash)
    except Exception as e:
        msg = (
            "Error cherry-picking: %s\nWould you like to manually fix-up this merge?"
            % e
        )
        continue_maybe(msg)
        msg = "Okay, please fix any conflicts and finish the cherry-pick. Finished?"
        continue_maybe(msg)

    continue_maybe(
        "Pick complete (local ref %s). Push to %s?"
        % (pick_branch_name, PUSH_REMOTE_NAME)
    )

    try:
        run_cmd("git push %s %s:%s" % (PUSH_REMOTE_NAME, pick_branch_name, pick_ref))
    except Exception as e:
        clean_up()
        fail("Exception while pushing: %s" % e)

    pick_hash = run_cmd("git rev-parse %s" % pick_branch_name).strip()
    clean_up()

    print("Pull request #%s picked into %s!" % (pr_num, pick_ref))
    print("Pick hash: %s" % pick_hash)
    return pick_ref, pick_hash


def get_current_ref():
    ref = run_cmd("git rev-parse --abbrev-ref HEAD").strip()
    if ref == "HEAD":
        # The current ref is a detached HEAD, so grab its SHA.
        return run_cmd("git rev-parse HEAD").strip()
    else:
        return ref


def main():
    global original_head

    os.chdir(KYUUBI_HOME)
    original_head = get_current_ref()

    branches = get_json("%s/branches" % GITHUB_API_BASE)
    branch_names = list(
        filter(lambda x: x.startswith("branch-"), [x["name"] for x in branches])
    )
    # Sort release branches numerically, newest first.
    def sort_by_version(branch_name):
        return tuple(map(int, branch_name.split("-")[1].split(".")))

    branch_names = sorted(branch_names, key=sort_by_version, reverse=True)

    pr_num = get_input(
        "Which pull request would you like to merge? (e.g. 34): ", r"\d+"
    )
    pr = get_json("%s/pulls/%s" % (GITHUB_API_BASE, pr_num))
    pr_events = get_json("%s/issues/%s/events" % (GITHUB_API_BASE, pr_num))

    url = pr["url"]
    title = pr["title"]
    title = fix_title(title, pr_num)
    body = re.sub(re.compile(r"<!--[^>]*-->\n?", re.DOTALL), "", pr["body"]).lstrip()
    target_ref = pr["base"]["ref"]
    user_login = pr["user"]["login"]
    base_ref = pr["head"]["ref"]
    pr_repo_desc = "%s/%s" % (user_login, base_ref)
    assignees = pr["assignees"]
    milestone = pr["milestone"]

    pr_author_info = get_json("https://api.github.com/users/%s" % user_login)
    pr_author_name = pr_author_info.get("name") or user_login
    pr_author_email = pr_author_info.get("email")
    pr_commits = get_json("%s/pulls/%s/commits" % (GITHUB_API_BASE, pr_num))
    if not pr_author_email:
        for commit in pr_commits:
            commit_author = commit.get("author")
            if commit_author and commit_author.get("login") == user_login:
                pr_author_email = commit["commit"]["author"]["email"]
                break
    if not pr_author_email:
        pr_author_email = "%s+%s@users.noreply.github.com" % (
            pr_author_info["id"],
            user_login,
        )
    pr_author = "%s <%s>" % (pr_author_name, pr_author_email)

    co_authors = []
    seen_co_authors = set()
    for commit in pr_commits:
        commit_author = commit.get("author")
        if commit_author and commit_author.get("login") == user_login:
            continue
        raw_author = "%s <%s>" % (
            commit["commit"]["author"]["name"],
            commit["commit"]["author"]["email"],
        )
        if raw_author not in seen_co_authors:
            seen_co_authors.add(raw_author)
            co_authors.append(raw_author)

    merge_hash, message = (None, None)
    if pr["state"] == "closed":
        merge_hash, message = find_merge_commit(pr_num, pr_events)

    if merge_hash is not None:

        print(
            "Pull request %s has already been merged, assuming you want to backport"
            % pr_num
        )
        commit_is_downloaded = (
            run_cmd(
                ["git", "rev-parse", "--quiet", "--verify", "%s^{commit}" % merge_hash]
            ).strip()
            != ""
        )
        if not commit_is_downloaded:
            fail(
                "Couldn't find any merge commit for #%s, you may need to update HEAD."
                % pr_num
            )

        print("Found commit %s:\n%s" % (merge_hash, message))
        picked_refs = [target_ref]
        picked_commits = []
        try:
            while True:
                default_branch = default_pick_branch(branch_names, tuple(picked_refs))
                if default_branch is None:
                    print(
                        "Every known release branch already contains #%s; nothing to pick."
                        % pr_num
                    )
                    break
                picked = cherry_pick(pr_num, merge_hash, default_branch, branch_names)
                picked_refs = picked_refs + [picked[0]]
                picked_commits = picked_commits + [picked]
                prompt = "Would you like to pick %s into another branch?" % merge_hash
                if get_input("\n%s (y/N): " % prompt, ["y", "n", ""]) != "y":
                    break
        finally:
            post_merge_comment(pr_num, picked_commits)
        sys.exit(0)

    if not bool(pr["mergeable"]):
        msg = (
            "Pull request %s is not mergeable in its current form.\n" % pr_num
            + "Continue? (experts only!)"
        )
        continue_maybe(msg)

    print("\n=== Pull Request #%s ===" % pr_num)
    print(
        "title:\t%s\nsource:\t%s\ntarget:\t%s\nurl:\t%s\nbody:\n\n%s"
        % (title, pr_repo_desc, target_ref, url, body)
    )

    if assignees is None or len(assignees) == 0:
        continue_maybe("Assignees have NOT been set. Continue?")
    else:
        print("assignees: %s" % [assignee["login"] for assignee in assignees])

    if milestone is None:
        continue_maybe("Milestone has NOT been set. Continue?")
    else:
        print("milestone: %s" % milestone["title"])

    continue_maybe("Proceed with merging pull request #%s?" % pr_num)

    merged_refs = [target_ref]

    merge_hash = merge_pr(
        pr_num, target_ref, title, body, pr_repo_desc, pr_author, co_authors
    )
    merged_commits = [(target_ref, merge_hash)]

    pick_prompt = "Would you like to pick %s into another branch?" % merge_hash
    try:
        while get_input("\n%s (y/N): " % pick_prompt, ["y", "n", ""]) == "y":
            default_branch = default_pick_branch(branch_names, tuple(merged_refs))
            if default_branch is None:
                print(
                    "Every known release branch already contains #%s; nothing to pick."
                    % pr_num
                )
                break
            picked = cherry_pick(pr_num, merge_hash, default_branch, branch_names)
            merged_refs = merged_refs + [picked[0]]
            merged_commits = merged_commits + [picked]
    finally:
        pr_state = get_json("%s/pulls/%s" % (GITHUB_API_BASE, pr_num)).get("state")
        if pr_state != "closed":
            print("PR #%s is still open after push; closing it explicitly." % pr_num)
            close_pr(pr_num)
        post_merge_comment(pr_num, merged_commits)


if __name__ == "__main__":
    import doctest

    (failure_count, test_count) = doctest.testmod()
    if failure_count:
        sys.exit(-1)
    try:
        main()
    except:
        clean_up()
        raise
