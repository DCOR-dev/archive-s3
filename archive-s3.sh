#!/bin/bash
# Create a cron job for this bash script with the following content:
#
#   ARCHIVE_MAIL=mymail@example.com
#   0 */6 * * * ~/repos/archive-s3/archive-s3.sh >> /dev/null 2>&1
#
LOGFILE=$(mktemp)

# run the python script
source ~/env_archive_s3/bin/activate
python ~/repos/archive-s3/archive-s3.py > "$LOGFILE" 2>&1

# check for errors
retVal=$?
if [ $retVal -ne 0 ] && [ -n "$ARCHIVE_MAIL" ]; then
    # if errors occurred and mail address is specified, send email
    /usr/bin/mail -s "S3 archive errors" "$ARCHIVE_MAIL" < "$LOGFILE"
fi

# cleanup
rm -f "$LOGFILE"

exit $retVal
