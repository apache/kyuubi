#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Generates table of content for markdown.
Your title style must be like this:
    <h1 id="h1">H1 title</h1>
    <h2 id="h2">H2 title</h2>
    ...
Generated TOC like this:
    *   [H1 title](#h1)
        *    [H2 title](#h2)
    ...
usage: toc_gen.py [-h] [-S src] [-D des]
Generates TOC for markdown file.
optional arguments:
      -h, --help  show this help message and exit
      -S src      A path of source file.
      -D des      A file path to store TOC.
"""

from __future__ import print_function

import os
import argparse
from HTMLParser import HTMLParser

def get_toc(html):

    toc_list = []

    class MyHTMLParser(HTMLParser):

        _prefix = ''
        _id = ''
        _title = ''

        def handle_starttag(self, tag, attrs):
            if tag[-1].isdigit():
                space = (int(tag[-1]) - 1) * 4
                self._prefix = space * ' ' + '*   '
            attrs = dict(attrs)
            if self._prefix and 'id' in attrs:
                self._id = '(#' + attrs['id'] + ')'

        def handle_data(self, data):
            if self._prefix:
                self._title = '[' + data.strip() + ']'
                toc_list.append(self._prefix + self._title + self._id)
            self._prefix = ''
            self._id = ''
            self._title = ''

    parser = MyHTMLParser()
    parser.feed(html)
    return '\n'.join(toc_list)

def read(fpath):
    with open(fpath, 'r') as f:
        data = f.read()
    return data

def write(fpath, toc):
    with open(fpath, 'w') as f:
        f.write(toc)

def parse_args():
    parser = argparse.ArgumentParser(
        description = "Generates TOC for markdown file.")
    parser.add_argument(
        '-S',
        type = file_check,
        default = None,
        help = "A path of source file.",
        metavar = 'src',
        dest = 'src')
    parser.add_argument(
        '-D',
        type = path_check,
        default = None,
        help = "A file path to store TOC.",
        metavar = 'des',
        dest = 'des')
    args = parser.parse_args()
    return args.src, args.des

def file_check(fpath):
    if os.path.isfile(fpath):
        return fpath
    raise argparse.ArgumentTypeError("Invalid source file path,"
                                     " {0} doesn't exists.".format(fpath))

def path_check(fpath):
    if fpath is None: return
    path = os.path.dirname(fpath)
    if os.path.exists(path):
        return fpath
    raise argparse.ArgumentTypeError("Invalid destination file path,"
                                     " {0} doesn't exists.".format(fpath))


def main():
    src, des = parse_args()
    toc = get_toc(read(src))
    if des:
        write(des, toc)
        print("TOC of '{0}' has been written to '{1}'".format(
            os.path.abspath(src),
            os.path.abspath(des)))
    else:
        print("TOC for '{0}':\n '{1}'".format(
            os.path.abspath(src),
            toc))

if __name__ == '__main__':
    main()