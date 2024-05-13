#!/bin/bash
# Create a cron job for this bash script with the following content:
#
#   * */6 * * * ~/repos/archive-s3/archive-s3.sh >> /dev/null 2>&1
#
set -e
set -x

source source ~/env_archive_s3/bin/activate
python ~/repos/archive-s3/archive-s3.py
