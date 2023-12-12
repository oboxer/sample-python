import argparse
from typing import Dict

import polars as pl
import ray
import s3fs

from ctizh.dataeng.transform.record import Record


@ray.remote
def create_record_parallel(row: Dict) -> Dict:
    # Parallelize the creation of Record instances using Ray
    return Record.parse_obj(row).dict(exclude_defaults=True)


def extract_and_transform(
    aws_access_key: str,
    aws_secret_key: str,
    input_csv_path: str,
    output_parquet_dir: str,
    num_cpus: int = 1,
) -> None:
    # Read data from S3
    fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)
    with fs.open(input_csv_path) as f:
        df = pl.read_csv(f)

    # Initialize Ray
    ray.init(num_cpus=num_cpus)

    # Apply the function to each row using Ray to parallelize the operation
    records = ray.get([create_record_parallel.remote(row) for row in df.rows(named=True)])

    # Remove duplicates using all cols
    deduplicated_df = pl.from_records(records).unique(subset=Record.get_unique_keys())
    # handel cases where transform fails
    if "err_msg" in deduplicated_df.columns:
        valid_df = deduplicated_df.filter(pl.col("err_msg").is_null()).select(pl.exclude("err_msg"))
        invalid_df = deduplicated_df.filter(pl.col("err_msg").is_not_null())
    else:
        valid_df = deduplicated_df
        invalid_df = None

    # Write data to S3
    with fs.open(f"{output_parquet_dir}/valid/valid_output.parquet", mode="wb") as f:
        # uncompressed is needed b/c redshift spectrum has problems reading zstd compression
        valid_df.write_parquet(f, compression="uncompressed")
    if invalid_df:
        with fs.open(f"{output_parquet_dir}/invalid/err_output.parquet", mode="wb") as f:
            invalid_df.write_parquet(f, compression="uncompressed")

    # Shut down Ray
    ray.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract and Transform data")

    parser.add_argument("--aws-access-key", type=str, help="AWS Access Key", required=True)
    parser.add_argument("--aws-secret-key", type=str, help="AWS Secret Key", required=True)
    parser.add_argument("--input-csv-path", type=str, help="Input CSV file path", required=True)
    parser.add_argument("--output-parquet-dir", type=str, help="Output Parquet directory", required=True)
    parser.add_argument("--num-cpus", type=int, default=1, help="Number of CPUs (default: 1)")

    args = parser.parse_args()

    extract_and_transform(
        args.aws_access_key,
        args.aws_secret_key,
        args.input_csv_path,
        args.output_parquet_dir,
        args.num_cpus,
    )


if __name__ == "__main__":
    main()
