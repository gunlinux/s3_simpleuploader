import os
import logging
import sys
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)
load_dotenv()


def upload_files(client, bucket_name, *args) -> None:
    # Make the bucket if it doesn't exist.
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        logger.info("Created bucket %s", bucket_name)
    else:
        logger.info("Bucket %s already exists", bucket_name)

    for file in args:
        # Upload the file, renaming it in the process
        filename = os.path.basename(file)
        client.fput_object(
            bucket_name, filename, file,
        )
        logger.info(
            "%s successfully uploaded as object %s to bucket %s", file,
            filename, bucket_name)


def main():
    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio(
        os.environ.get("host"),
        access_key=os.environ.get('access_key'),
        secret_key=os.environ.get('secret_key'),
        secure=False,
    )

    bucket_name = "tesbackuet"
    source_file = "/home/loki/projects/s3/testfile.txt"
    upload_files(client, bucket_name, source_file)


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        logger.critical("error occurred %s", exc)
