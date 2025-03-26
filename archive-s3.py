import atexit
import pathlib
import re
import shutil
import sys
import tempfile
import time
import warnings
import zipfile

import boto3
import psutil


class ReachingQuotaLimitError(BaseException):
    """used when reaching the quota limit"""


class SmallObjectPacker:
    def __init__(self, output_path, bucket_name, s3_client, min_file_size):
        """Helper class for packing small objects into an uncompressed zip"""
        output_path = pathlib.Path(output_path)
        self.path_arc = (
                output_path / bucket_name / "small_objects" /
                time.strftime(f"small_objects_{bucket_name}_%Y-%d-%m.zip"))
        self.path_arc.parent.mkdir(parents=True, exist_ok=True)
        self.arc = zipfile.ZipFile(self.path_arc,
                                   mode="a",
                                   # disable compression
                                   compression=zipfile.ZIP_STORED,
                                   allowZip64=True
                                   )
        self.bucket_name = bucket_name
        self.s3_client = s3_client
        self.min_file_size = min_file_size
        self.tdir = pathlib.Path(tempfile.mkdtemp(prefix="archive_object"))
        atexit.register(shutil.rmtree, self.tdir, ignore_errors=True)
        # get a list of all previously loaded files in this bucket
        self.file_list = []
        for pp in self.path_arc.parent.glob("small_objects_*.txt"):
            lines = pp.read_text().split("\n")
            self.file_list += [ll.strip() for ll in lines if ll.strip()]

    def add_object(self, object_name):
        # Check whether the object is already in an archive
        zip_name = f"{self.bucket_name}/{object_name}"
        if zip_name in self.file_list:
            # We already archived this file before
            retval = 0
        else:
            try:
                # make sure the file is really not already in the zip file
                self.arc.getinfo(zip_name)
            except KeyError:
                # Not in archive -> download
                retval = download_resource(
                    bucket_name=self.bucket_name,
                    object_name=object_name,
                    output_path=self.tdir,
                    s3_client=self.s3_client,
                   )
                object_path = self.tdir / self.bucket_name / object_name
                self.arc.write(object_path, zip_name)
                object_path.unlink()
            else:
                retval = 0
        return retval

    def close(self):
        # get list of items
        files = self.arc.namelist()
        if files:
            # Write a list of files added to the archive
            path_txt = self.path_arc.with_suffix(".txt")
            path_txt.write_text("\n".join(self.arc.namelist()))

            if self.path_arc.stat().st_size < self.min_file_size:
                # Make sure the file size is larger than self.min_file_size
                # by writing a file of size self.min_file_size.
                dummy_file = self.tdir / "dummy"
                dummy_file.write_bytes(b"0"*self.min_file_size)
                self.arc.write(dummy_file, "dummy.img")

            self.arc.close()

            if self.path_arc.stat().st_size < self.min_file_size:
                warnings.warn(f"The file {self.path_arc} is smaller than the "
                              f"minimum file size {self.min_file_size}. This "
                              f"should not have happened!")
        else:
            # delete archive that does not have any content
            self.arc.close()
            self.path_arc.unlink()
        # cleanup
        shutil.rmtree(self.tdir, ignore_errors=True)


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
        retval = path_backup.stat().st_size
    else:
        retval = 0
    return retval


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


def run_archive(pc, verbose=True):
    config = get_config(pc)
    config["object_size_min"] = int(config["object_size_min"])
    config["s3_quota"] = int(config["s3_quota"])
    print("Archiving ", config["name"])
    s3_client = get_s3_client(config)
    re_bucket = re.compile(config["regexp_bucket"])
    re_object = re.compile(config["regexp_object"])
    num_buckets_archived = 0
    num_buckets_ignored = 0
    num_objects_archived = 0
    num_objects_archived_small = 0
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

        bucket_box = SmallObjectPacker(
            output_path=config["archive_path"],
            bucket_name=bucket_name,
            s3_client=s3_client,
            min_file_size=config["object_size_min"]
        )

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
                    if object_size < config["object_size_min"]:
                        size_archived += bucket_box.add_object(
                            object_name=object_name)
                        num_objects_archived_small += 1
                    else:
                        size_archived += download_resource(
                            bucket_name=bucket_name,
                            object_name=object_name,
                            output_path=config["archive_path"],
                            s3_client=s3_client,
                        )
                    num_objects_archived += 1
                    if verbose and num_objects_archived % 100 == 0:
                        print(f"Fetched: {num_objects_archived} files, "
                              f"{size_archived / 1024 ** 3:.1f} GiB",
                              end="\r")
                else:
                    num_objects_ignored_regexp += 1

            print(f"Fetched: {num_objects_archived} files, "
                  f"{size_archived / 1024 ** 3:.1f} GiB")

            if not resp.get("IsTruncated"):
                break
            else:
                kwargs["ContinuationToken"] = resp.get(
                    "NextContinuationToken")

        # Make sure small files from this bucket are archived as well
        bucket_box.close()

    quota_percent = size_total / config["s3_quota"]

    print(f"""\nSummary:
    Buckets archived: {num_buckets_archived}
    Buckets ignored: {num_buckets_ignored}
    Objects archived total: {num_objects_archived}
    Objects archived small: {num_objects_archived_small}
    Objects ignored due to regexp: {num_objects_ignored_regexp}
    Total archive size: {size_total/1024**3:.0f} GiB ({quota_percent:.0%})
    Added to the archive: {size_archived/1024**3:.0f} GiB
    """)

    ret_dict = {
        "name": config["name"],
        "s3_quota_used": quota_percent,
    }
    return ret_dict


def unlink_file_missing_ok(path):
    if path.exists():
        path.unlink()


if __name__ == "__main__":
    if get_lock():
        quota_issues = []
        # get configuration files
        here = pathlib.Path(__file__).parent
        for pc in (here / "conf.d").glob("*.conf"):
            rd = run_archive(pc, verbose="report" not in sys.argv)
            if rd["s3_quota_used"] > 0.95:
                quota_issues.append(rd["name"])

        if quota_issues:
            raise ReachingQuotaLimitError(
                f"Getting close to the quota limit for {quota_issues}! "
                f"Please check the current quota limits in the configuration "
                f"file and/or request a higher quota limit."
                )
