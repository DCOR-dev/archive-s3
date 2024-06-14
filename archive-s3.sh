#!/bin/bash
# Create a cron job for this bash script with the following content:
#
#   * */6 * * * ~/repos/archive-s3/archive-s3.sh >> /dev/null 2>&1
#
# Or, if you can send emails, do this:
#
#   * */6 * * * ~/repos/archive-s3/archive-s3.sh 2> >(/usr/bin/mail -s "S3 archive errors" recipient@example.com)
#
set -e
set -x

source source ~/env_archive_s3/bin/activate
python ~/repos/archive-s3/archive-s3.py
