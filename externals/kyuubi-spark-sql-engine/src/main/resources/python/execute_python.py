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
import datetime
import decimal
import io
import json

import os
import re
import sys
import traceback
import base64
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

MAGIC_ENABLED = os.environ.get("MAGIC_ENABLED") == "true"


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


class UnknownMagic(Exception):
    pass


class MagicNode(object):
    def __init__(self, line):
        parts = line[1:].split(" ", 1)
        if len(parts) == 1:
            self.magic, self.rest = parts[0], ()
        else:
            self.magic, self.rest = parts[0], (parts[1],)

    def execute(self):
        if not self.magic:
            raise UnknownMagic("magic command not specified")

        try:
            handler = magic_router[self.magic]
        except KeyError:
            raise UnknownMagic("unknown magic command '%s'" % self.magic)

        try:
            return handler(*self.rest)
        except ExecutionError as e:
            raise e
        except Exception:
            exc_type, exc_value, tb = sys.exc_info()
            raise ExecutionError((exc_type, exc_value, None))


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
        # It's possible we hit a syntax error because of a magic command. Split the code groups
        # of 'normal code', and code that starts with a '%'. possibly magic code lines, and see
        # if any of the lines. Remove lines until we find a node that parses, then check if the
        # next line is a magic line.

        # Split the code into chunks of normal code, and possibly magic code, which starts with
        # a '%'.

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
            if MAGIC_ENABLED and chunk.startswith("%"):
                nodes.append(MagicNode(chunk))
            else:
                nodes.append(NormalNode(chunk))

    return nodes


def execute_reply(status, content):
    return {
        "msg_type": "execute_reply",
        "content": dict(
            content,
            status=status,
        ),
    }


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
    except UnknownMagic:
        exc_type, exc_value, tb = sys.exc_info()
        return execute_reply_error(exc_type, exc_value, None)
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


def magic_table_convert(value):
    try:
        converter = magic_table_types[type(value)]
    except KeyError:
        converter = magic_table_types[str]

    return converter(value)


def magic_table_convert_seq(items):
    last_item_type = None
    converted_items = []

    for item in items:
        item_type, item = magic_table_convert(item)

        if last_item_type is None:
            last_item_type = item_type
        elif last_item_type != item_type:
            raise ValueError("value has inconsistent types")

        converted_items.append(item)

    return "ARRAY_TYPE", converted_items


def magic_table_convert_map(m):
    last_key_type = None
    last_value_type = None
    converted_items = {}

    for key, value in m:
        key_type, key = magic_table_convert(key)
        value_type, value = magic_table_convert(value)

        if last_key_type is None:
            last_key_type = key_type
        elif last_value_type != value_type:
            raise ValueError("value has inconsistent types")

        if last_value_type is None:
            last_value_type = value_type
        elif last_value_type != value_type:
            raise ValueError("value has inconsistent types")

        converted_items[key] = value

    return "MAP_TYPE", converted_items


magic_table_types = {
    type(None): lambda x: ("NULL_TYPE", x),
    bool: lambda x: ("BOOLEAN_TYPE", x),
    int: lambda x: ("INT_TYPE", x),
    float: lambda x: ("DOUBLE_TYPE", x),
    str: lambda x: ("STRING_TYPE", str(x)),
    datetime.date: lambda x: ("DATE_TYPE", str(x)),
    datetime.datetime: lambda x: ("TIMESTAMP_TYPE", str(x)),
    decimal.Decimal: lambda x: ("DECIMAL_TYPE", str(x)),
    tuple: magic_table_convert_seq,
    list: magic_table_convert_seq,
    dict: magic_table_convert_map,
}


def magic_table(name):
    try:
        value = global_dict[name]
    except KeyError:
        exc_type, exc_value, tb = sys.exc_info()
        raise ExecutionError((exc_type, exc_value, None))

    if not isinstance(value, (list, tuple)):
        value = [value]

    headers = {}
    data = []

    for row in value:
        cols = []
        data.append(cols)

        if "Row" == row.__class__.__name__:
            row = row.asDict()

        if not isinstance(row, (list, tuple, dict)):
            row = [row]

        if isinstance(row, (list, tuple)):
            iterator = enumerate(row)
        else:
            iterator = sorted(row.items())

        for name, col in iterator:
            col_type, col = magic_table_convert(col)

            try:
                header = headers[name]
            except KeyError:
                header = {
                    "name": str(name),
                    "type": col_type,
                }
                headers[name] = header
            else:
                # Reject columns that have a different type. (allow none value)
                if col_type != "NULL_TYPE" and header["type"] != col_type:
                    if header["type"] == "NULL_TYPE":
                        header["type"] = col_type
                    else:
                        exc_type = Exception
                        exc_value = Exception("table rows have different types")
                        raise ExecutionError((exc_type, exc_value, None))

            cols.append(col)

    headers = [v for k, v in sorted(headers.items())]

    return {
        "application/vnd.livy.table.v1+json": {
            "headers": headers,
            "data": data,
        }
    }


def magic_json(name):
    try:
        value = global_dict[name]
    except KeyError:
        exc_type, exc_value, tb = sys.exc_info()
        raise ExecutionError((exc_type, exc_value, None))

    return {
        "application/json": value,
    }


def magic_matplot(name):
    try:
        value = global_dict[name]
        fig = value.gcf()
        imgdata = io.BytesIO()
        fig.savefig(imgdata, format="png")
        imgdata.seek(0)
        encode = base64.b64encode(imgdata.getvalue())
        if sys.version >= "3":
            encode = encode.decode()

    except:
        exc_type, exc_value, tb = sys.exc_info()
        raise ExecutionError((exc_type, exc_value, None))

    return {
        "image/png": encode,
    }


magic_router = {
    "table": magic_table,
    "json": magic_json,
    "matplot": magic_matplot,
}


# get or create spark session
spark_session = kyuubi_util.get_spark_session(
    os.environ.get("KYUUBI_SPARK_SESSION_UUID")
)
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
            try:
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

                try:
                    result = json.dumps(result)
                except ValueError:
                    result = json.dumps(
                        {
                            "msg_type": "inspect_reply",
                            "content": {
                                "status": "error",
                                "ename": "ValueError",
                                "evalue": "cannot json-ify %s" % result,
                                "traceback": [],
                            },
                        }
                    )
                except Exception:
                    exc_type, exc_value, tb = sys.exc_info()
                    result = json.dumps(
                        {
                            "msg_type": "inspect_reply",
                            "content": {
                                "status": "error",
                                "ename": str(exc_type.__name__),
                                "evalue": "cannot json-ify %s: %s"
                                % (result, str(exc_value)),
                                "traceback": [],
                            },
                        }
                    )

                print(result, file=sys_stdout)
            except KeyboardInterrupt:
                result = json.dumps(
                    {
                        "msg_type": "inspect_reply",
                        "content": {
                            "status": "canceled",
                            "ename": "KeyboardInterrupt",
                            "evalue": "execution interrupted by user",
                            "traceback": [],
                        },
                    }
                )
                print(result, file=sys_stdout)
                print("execution interrupted by user: " + line, file=sys_stderr)
            sys_stdout.flush()
            clearOutputs()
    finally:
        print("python worker exit", file=sys_stderr)
        sys.stdin = sys_stdin
        sys.stdout = sys_stdout
        sys.stderr = sys_stderr


if __name__ == "__main__":
    sys.exit(main())
