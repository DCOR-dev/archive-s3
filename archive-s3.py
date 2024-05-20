import atexit
import pathlib
import re
import time

import boto3
import psutil


def download_resource(bucket_name, object_name, output_path, s3_client):
    """Download an object from S3, skipping already existing files"""
    path_backup = pathlib.Path(output_path) / bucket_name / object_name
    if not path_backup.exists():
        # temporary file for downloading
        path_temp = path_backup.with_name("temp_" + path_backup.name + "~")
        if path_temp.exists():
            path_temp.unlink()
        path_temp.parent.mkdir(parents=True, exist_ok=True)
        # perform download
        s3_client.download_file(bucket_name, object_name, str(path_temp))
        path_temp.rename(path_backup)
        return path_backup.stat().st_size
    else:
        return 0


def get_config(path):
    """Return configuration dictionary from file"""
    config = {}
    for line in path.read_text().split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.count("="):
            key, value = line.split("=", 1)
            config[key.strip()] = value.strip()
    return config


def get_lock():
    lock_file = pathlib.Path(__file__).with_suffix(".lock")
    # Prevent this script from being run twice
    if lock_file.exists():
        # Check the date of the lock file
        if time.time() - lock_file.stat().st_ctime > 7200:
            # Check whether there are other processes running
            proc_ident = "archive-s3"
            count = 0
            for pc in psutil.process_iter():
                count += "".join(pc.cmdline()).count(proc_ident)
            if count > 1:
                # Another process is still running
                print(f"Other process is using {lock_file}, exiting!")
                return
        else:
            print(f"Lock file {lock_file} exists, exiting!")
            return
        # If we got here, that means that the lock file is too old and no other
        # process is currently running.
        pass

    # Register lock file
    lock_file.touch()
    atexit.register(unlink_file_missing_ok, lock_file)
    return True


def get_s3_client(config):
    """Return the current S3 client"""
    # Create a new session (do not use the default session)
    s3_session = boto3.Session(
        aws_access_key_id=config["s3_access_key_id"],
        aws_secret_access_key=config["s3_secret_access_key"],
    )
    s3_client = s3_session.client(
        service_name='s3',
        use_ssl=True,
        verify=True,
        endpoint_url=config["s3_endpoint_url"],
    )
    return s3_client


def run_archive(pc):
    config = get_config(pc)
    print("Archiving ", config["name"])
    s3_client = get_s3_client(config)
    re_bucket = re.compile(config["regexp_bucket"])
    re_object = re.compile(config["regexp_object"])
    num_buckets_archived = 0
    num_buckets_ignored = 0
    num_objects_archived = 0
    num_objects_ignored_size = 0
    num_objects_ignored_regexp = 0
    size_total = 0
    size_archived = 0

    # fetch a list of buckets
    buckets = [b["Name"] for b in s3_client.list_buckets()["Buckets"]]
    # iterate through all objects
    for bucket_name in buckets:
        if re_bucket.match(bucket_name):
            num_buckets_archived += 1
        else:
            num_buckets_ignored += 1

        kwargs = {"Bucket": bucket_name,
                  "MaxKeys": 500
                  }
        while True:
            resp = s3_client.list_objects_v2(**kwargs)

            for obj in resp.get("Contents", []):
                object_name = obj["Key"]
                object_size = obj["Size"]

                if re_object.match(object_name):
                    size_total += object_size
                    if object_size < int(config["object_size_min"]):
                        num_objects_ignored_size += 1
                    else:
                        size_archived += download_resource(
                            bucket_name=bucket_name,
                            object_name=object_name,
                            output_path=config["archive_path"],
                            s3_client=s3_client,
                        )
                        num_objects_archived += 1
                        print(f"Archiving: {num_objects_archived} files,"
                              f" {size_archived / 1024 ** 3:.1f} GiB",
                              end="\r")
                else:
                    num_objects_ignored_regexp += 1

            if not resp.get("IsTruncated"):
                break
            else:
                kwargs["ContinuationToken"] = resp.get(
                    "NextContinuationToken")

    print(f"""Summary:
    Buckets archived: {num_buckets_archived}
    Buckets ignored: {num_buckets_ignored}
    Objects archived: {num_objects_archived}
    Objects ignored due to regexp: {num_objects_ignored_regexp}
    Objects ignored due to size: {num_objects_ignored_size}
    Total archive size: {size_total/1024**3:.0f} GiB
    Added to the archive: {size_archived/1024**3:.0f} GiB
    """)


def unlink_file_missing_ok(path):
    if path.exists():
        path.unlink()


if __name__ == "__main__":
    if get_lock():
        # get configuration files
        here = pathlib.Path(__file__).parent
        for pc in (here / "conf.d").glob("*.conf"):
            run_archive(pc)
