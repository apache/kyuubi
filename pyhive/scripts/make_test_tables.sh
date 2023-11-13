#!/bin/bash -eux
# Hive must be on the path for this script to work.
# WARNING: drops and recreates tables called one_row, one_row_complex, and many_rows, plus a
# database called pyhive_test_database.

$(dirname $0)/make_one_row.sh
$(dirname $0)/make_one_row_complex.sh
$(dirname $0)/make_many_rows.sh
$(dirname $0)/make_test_database.sh
