archive-s3
==========

This repository contains scripts for archiving data located on S3.
Everything is maintained via a configuration files in the `conf.d`
directory.

Create a Python environment and install the requirements::

    python3 -m venv env_archive_s3
    source env_archive_s3/bin/activate
    pip install -r requirements.txt

Create configuration files from the template and run the script::

    python archive-s3.py

After an initial sync, you can create a cron job using `crontab -e`::

    # run archiving script every six hours
    * */6 * * * ~/repos/archive-s3/archive-s3.sh >> /dev/null 2>&1
