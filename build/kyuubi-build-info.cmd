@echo off

rem
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem    http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.
rem

SET EXTRA_RESOURCE_DIR=%~1
IF NOT EXIST "%EXTRA_RESOURCE_DIR%" MKDIR "%EXTRA_RESOURCE_DIR%"
SET BUILD_INFO="%EXTRA_RESOURCE_DIR%"\kyuubi-version-info.properties
CALL :echo_build_properties %* > "%BUILD_INFO%"

EXIT /B %ERRORLEVEL%

:echo_build_properties
echo kyuubi_version=%~2
echo kyuubi_java_version=%~3
echo kyuubi_scala_version=%~4
echo kyuubi_spark_version=%~5
echo kyuubi_hive_version=%~6
echo kyuubi_hadoop_version=%~7
echo kyuubi_flink_version=%~8
echo kyuubi_trino_version=%~9
echo user=%username%

FOR /F %%i IN ('git rev-parse HEAD') DO SET "revision=%%i"
FOR /F %%i IN ('git rev-parse --abbrev-ref HEAD') DO SET "branch=%%i"
FOR /F %%i IN ('git config --get remote.origin.url') DO SET "url=%%i"

FOR /f %%i IN ('date /t') DO SET current_date=%%i
FOR /f %%i IN ("%TIME%") DO SET current_time=%%i
set date=%current_date%_%current_time%

echo revision=%revision%
echo branch=%branch%
echo date=%date%
echo url=%url%
GOTO:EOF
