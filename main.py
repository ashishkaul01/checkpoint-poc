from deltalake import DeltaTable, write_deltalake
import boto3
from botocore.client import Config
import polars as pl

def main():

    # Define storage options for Minio
    storage_options = {
        "AWS_ACCESS_KEY_ID": "VPP0fkoCyBZx8YU0QTjH",
        "AWS_SECRET_ACCESS_KEY": "iFq6k8RLJw5B0faz0cKCXeQk0w9Q8UdtaFzHuw4J",
        "AWS_ENDPOINT_URL": "http://localhost:9000",
        "AWS_REGION": "us-west-rack-2",
        "AWS_STORAGE_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_ALLOW_HTTP": "true",
    }

    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='VPP0fkoCyBZx8YU0QTjH',
        aws_secret_access_key='iFq6k8RLJw5B0faz0cKCXeQk0w9Q8UdtaFzHuw4J',
        region_name='us-west-rack-2'
    )

    bucket_name = 'pai-bucket'
    file_name = 'version_789456'

    # Read the file from MinIO
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        file_content = response['Body'].read().decode('utf-8')
        starting_ver = int(file_content)
    except s3_client.exceptions.NoSuchKey:
        print(f'The specified key "{file_name}" does not exist.')
        starting_ver = 0

    print("Starting Version: ", starting_ver)
    dt = DeltaTable("s3a://sdp-bucket/delta-table", storage_options=storage_options)
    cdf = _load_change_data_feed_from_delta_table(dt, starting_version=starting_ver)
    cdf_version = cdf["_commit_version"].max()

    # Define the file name and content
    
    content = str(cdf_version)
    # Write content to the file
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=content)
    print("CDF Version: ", cdf_version)

def _load_change_data_feed_from_delta_table(
        deltatable: DeltaTable, starting_version: int = 0, ending_version: int | None = None
    ) -> pl.DataFrame | None:
        """Load the change feed from the delta table."""
        change_data_feed = deltatable.load_cdf(
            starting_version=starting_version, ending_version=ending_version
        ).read_all()
        change_data_feed_polar_table = pl.from_arrow(change_data_feed)
        if isinstance(change_data_feed_polar_table, pl.Series):
            change_data_feed_polar_table = change_data_feed_polar_table.to_frame()
        return change_data_feed_polar_table.group_by("_commit_version").len().sort("len", descending=True)

if __name__ == "__main__":
    main()
