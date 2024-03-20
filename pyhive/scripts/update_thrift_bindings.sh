#!/bin/bash -eux

HIVE_VERSION='2.3.0'  # Must be a released version

# Create a temporary directory
scriptdir=`dirname $0`
tmpdir=$scriptdir/.thrift_gen

# Clean up previous generation attempts, in case it breaks things
rm -rf $tmpdir
mkdir $tmpdir

# Download TCLIService.thrift from Hive
curl -o $tmpdir/TCLIService.thrift \
    https://raw.githubusercontent.com/apache/hive/rel/release-$HIVE_VERSION/service-rpc/if/TCLIService.thrift

# Apply patch that adds legacy GetLog methods
patch -d $tmpdir < $scriptdir/thrift-patches/TCLIService.patch

thrift -r --gen py -out $scriptdir/../ $tmpdir/TCLIService.thrift
rm $scriptdir/../__init__.py
