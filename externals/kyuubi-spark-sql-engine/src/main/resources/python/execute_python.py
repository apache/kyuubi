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

import ast
import io
import json

import os
import re
import sys
import traceback
from glob import glob

if sys.version_info[0] < 3:
    sys.exit("Python < 3 is unsupported.")

os.environ["PYSPARK_PYTHON"] = os.environ.get("PYSPARK_PYTHON", sys.executable)

# add pyspark to sys.path

if "pyspark" not in sys.modules:
    # try to get PY4J_PATH and use it directly if not none
    py4j_path = os.environ.get("PY4J_PATH")
    if py4j_path is not None:
        sys.path[:0] = sys_path = [py4j_path]
    else:
        spark_home = os.environ.get("SPARK_HOME", "")
        spark_python = os.path.join(spark_home, "python")
        try:
            py4j = glob(os.path.join(spark_python, "lib", "py4j-*.zip"))[0]
        except IndexError:
            raise Exception(
                "Unable to find py4j in {}, your SPARK_HOME may not be configured correctly".format(
                    spark_python
                )
            )
        sys.path[:0] = sys_path = [spark_python, py4j]
else:
    # already imported, no need to patch sys.path
    sys_path = None

# import kyuubi_util after preparing sys.path
import kyuubi_util

# ast api is changed after python 3.8, see https://github.com/ipython/ipython/pull/11593
if sys.version_info >= (3, 8):
    from ast import Module
else:
    # mock the new API, ignore second argument
    # see https://github.com/ipython/ipython/issues/11590
    from ast import Module as OriginalModule

    Module = lambda nodelist, type_ignores: OriginalModule(nodelist)

TOP_FRAME_REGEX = re.compile(r'\s*File "<stdin>".*in <module>')

global_dict = {}


class NormalNode(object):
    def __init__(self, code):
        self.code = compile(code, "<stdin>", "exec", ast.PyCF_ONLY_AST, 1)

    def execute(self):
        to_run_exec, to_run_single = self.code.body[:-1], self.code.body[-1:]

        try:
            for node in to_run_exec:
                mod = Module([node], [])
                code = compile(mod, "<stdin>", "exec")
                exec(code, global_dict)

            for node in to_run_single:
                mod = ast.Interactive([node])
                code = compile(mod, "<stdin>", "single")
                exec(code, global_dict)
        except Exception:
            # We don't need to log the exception because we're just executing user
            # code and passing the error along.
            raise ExecutionError(sys.exc_info())


class ExecutionError(Exception):
    def __init__(self, exc_info):
        self.exc_info = exc_info


class UnicodeDecodingStringIO(io.StringIO):
    def write(self, s):
        if isinstance(s, bytes):
            s = s.decode("utf-8")
        super(UnicodeDecodingStringIO, self).write(s)


def clearOutputs():
    sys.stdout.close()
    sys.stderr.close()
    sys.stdout = UnicodeDecodingStringIO()
    sys.stderr = UnicodeDecodingStringIO()


def parse_code_into_nodes(code):
    nodes = []
    try:
        nodes.append(NormalNode(code))
    except SyntaxError:
        normal = []
        chunks = []
        for i, line in enumerate(code.rstrip().split("\n")):
            if line.startswith("%"):
                if normal:
                    chunks.append("\n".join(normal))
                    normal = []

                chunks.append(line)
            else:
                normal.append(line)

        if normal:
            chunks.append("\n".join(normal))

        # Convert the chunks into AST nodes. Let exceptions propagate.
        for chunk in chunks:
            # TODO: look back here when Jupyter and sparkmagic are supported
            # if chunk.startswith('%'):
            #     nodes.append(MagicNode(chunk))

            nodes.append(NormalNode(chunk))

    return nodes


def execute_reply(status, content):
    msg = {
        "msg_type": "execute_reply",
        "content": dict(
            content,
            status=status,
        ),
    }
    return json.dumps(msg)


def execute_reply_ok(data):
    return execute_reply(
        "ok",
        {
            "data": data,
        },
    )


def execute_reply_error(exc_type, exc_value, tb):
    formatted_tb = traceback.format_exception(exc_type, exc_value, tb, chain=False)
    for i in range(len(formatted_tb)):
        if TOP_FRAME_REGEX.match(formatted_tb[i]):
            formatted_tb = formatted_tb[:1] + formatted_tb[i + 1 :]
            break

    return execute_reply(
        "error",
        {
            "ename": str(exc_type.__name__),
            "evalue": str(exc_value),
            "traceback": formatted_tb,
        },
    )


def execute_reply_internal_error(message, exc_info=None):
    return execute_reply(
        "error",
        {
            "ename": "InternalError",
            "evalue": message,
            "traceback": [],
        },
    )


def execute_request(content):
    try:
        code = content["code"]
    except KeyError:
        return execute_reply_internal_error(
            'Malformed message: content object missing "code"', sys.exc_info()
        )

    try:
        nodes = parse_code_into_nodes(code)
    except SyntaxError:
        exc_type, exc_value, tb = sys.exc_info()
        return execute_reply_error(exc_type, exc_value, None)

    result = None

    try:
        for node in nodes:
            result = node.execute()
    except ExecutionError as e:
        return execute_reply_error(*e.exc_info)

    if result is None:
        result = {}

    stdout = sys.stdout.getvalue()
    stderr = sys.stderr.getvalue()

    clearOutputs()

    output = result.pop("text/plain", "")

    if stdout:
        output += stdout

    if stderr:
        output += stderr

    output = output.rstrip()

    # Only add the output if it exists, or if there are no other mimetypes in the result.
    if output or not result:
        result["text/plain"] = output.rstrip()

    return execute_reply_ok(result)


# get or create spark session
spark_session = kyuubi_util.get_spark_session()
global_dict["spark"] = spark_session


def main():
    sys_stdin = sys.stdin
    sys_stdout = sys.stdout
    sys_stderr = sys.stderr

    sys.stdin = io.StringIO()
    sys.stdout = UnicodeDecodingStringIO()
    sys.stderr = UnicodeDecodingStringIO()

    stderr = sys.stderr.getvalue()
    print(stderr, file=sys_stderr)
    clearOutputs()

    try:

        while True:
            line = sys_stdin.readline()

            if line == "":
                break
            elif line == "\n":
                continue

            try:
                content = json.loads(line)
            except ValueError:
                continue

            if content["cmd"] == "exit_worker":
                break

            result = execute_request(content)
            print(result, file=sys_stdout)
            sys_stdout.flush()
            clearOutputs()
    finally:
        print("python worker exit", file=sys_stderr)
        sys.stdin = sys_stdin
        sys.stdout = sys_stdout
        sys.stderr = sys_stderr


if __name__ == "__main__":
    sys.exit(main())
