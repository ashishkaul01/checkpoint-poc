from deltalake import DeltaTable, write_deltalake
import pandas as pd

def main():

    df = pd.DataFrame({"id": [1, 2, 3], "value": ["let", "var", "const"]})

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

    try:
        # Write the DataFrame to a Delta table in Minio
        print("Writing DataFrame to Delta table...")
        write_deltalake("s3a://sdp-bucket/delta-table", df, storage_options=storage_options,configuration={"delta.enableChangeDataFeed": "true"}, mode="append")
        print("DataFrame written successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()