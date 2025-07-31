import os
import glob
import json
from argparse import ArgumentParser
from typing import Generator
import pandas as pd
import logging 

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) 

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file_path = os.path.join(log_dir, "application.log")

file_handler = logging.FileHandler(log_file_path)
console_handler = logging.StreamHandler()

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)


def main(ds_names: str | list | None, chunksize: int = 10000):
    """
    Orchestrates the data loading process from CSV files to the database.

    Args:
        ds_names: A single dataset name (str) or a list of dataset names (list),
                  or None to load all datasets found in the schema.
        chunksize: The number of rows to read from CSV files at a time.
    """
    env_var = {}
    src_base_dir = env_var["SRC_BASE_DIR"] = os.environ.get("SRC_BASE_DIR")
    env_var["DB_USER"] = os.environ.get("DB_USER")
    env_var["DB_PASS"] = os.environ.get("DB_PASS")
    env_var["DB_HOST"] = os.environ.get("DB_HOST")
    env_var["DB_PORT"] = os.environ.get("DB_PORT")
    env_var["DB_NAME"] = os.environ.get("DB_NAME")

    for var in env_var.keys():
        if not env_var[var]:
            # This should still raise a ValueError as it's a critical configuration issue
            raise ValueError(f"Missing environment variable {var}")
        
    db_conn_url = f"postgresql://{env_var["DB_USER"]}:{env_var["DB_PASS"]}@{env_var["DB_HOST"]}:{env_var["DB_PORT"]}/{env_var["DB_NAME"]}"
    try:
        schema_path: str = os.path.join(src_base_dir, "schemas.json")
        with open(schema_path, "r", encoding="utf-8") as schema:
            schemas: dict = json.load(schema)
        logger.info(f"Schema loaded successfully from {schema_path}")
    except json.JSONDecodeError as e:
        logger.critical(f"Unable to parse schema JSON to python Dict: {e}")
        raise 
    except FileNotFoundError as e:
        logger.critical(f"Unable to load schema. schema.json does not exist in {src_base_dir}")
        raise
    
    if not ds_names:
        ds_names = list(schemas.keys())
        logger.info(f"No dataset names provided. Loading all found datasets: {', '.join(ds_names)}")
    else:
        logger.info(f"Loading specified dataset(s): {ds_names}")


    if isinstance(ds_names, list):
        for ds_name in ds_names:
            db_loader(src_base_dir, schemas, db_conn_url, ds_name, chunksize)
    else:
        db_loader(src_base_dir, schemas, db_conn_url, ds_names, chunksize)


def db_loader(
    src_base_dir: str, schemas: dict, conn_url: str, ds_name: str, chunksize: int
):
    """
    Loads all CSV files for a specific dataset into the database.

    Args:
        src_base_dir: The base directory where data files are located.
        schemas: A dictionary containing schema definitions for all datasets.
        conn_url: The database connection URL.
        ds_name: The name of the dataset to load.
        chunksize: The number of rows per DataFrame chunk.
    """
    logger.info(f"Searching for CSV files for dataset: {ds_name} in {src_base_dir}/{ds_name}/")
    csv_files = glob.glob(f"{src_base_dir}/{ds_name}/part-*")
    if len(csv_files) == 0:
        logger.error(f"No files found for dataset {ds_name} at {src_base_dir}/{ds_name}/")
        raise ValueError(f"No files found for {ds_name}")

    for file in csv_files:
        logger.info(f"Starting processing of file: {file} for dataset {ds_name}")
        for idx, df in read_csv(file, schemas, ds_name, chunksize):
            try:
                logger.info(f"Processing chunk {idx} (rows: {df.shape[0]}) for {ds_name} from file: {os.path.basename(file)}")
                if to_sql(f"{ds_name}_test", df, conn_url):
                    logger.info(f"Chunk {idx} for {ds_name} processed successfully.")
            except Exception as e:
                logger.error(f"Failed to load chunk {idx} for {ds_name} from file {os.path.basename(file)}. Error: {e}", exc_info=True)
                continue
                
    logger.info(f"Finished processing all files for dataset: {ds_name}")


def read_csv(
    csv_path: str, schema: dict, ds_name: str, chunksize: int
) -> Generator[tuple[int, pd.DataFrame]]:
    """
    Reads a CSV file in chunks and yields DataFrames.

    Args:
        csv_path: The path to the CSV file.
        schema: A dictionary containing schema definitions.
        ds_name: The name of the dataset for column lookup.
        chunksize: The number of rows per DataFrame chunk.

    Yields:
        A tuple containing the chunk index (int) and the DataFrame chunk (pd.DataFrame).
    """
    try:
        col_name: list[str] = get_column_names(schema, ds_name)
        logger.debug(f"Reading CSV: {csv_path} with columns: {col_name}")
        df_reader = pd.read_csv(csv_path, names=col_name, chunksize=chunksize)
        for idx, df in enumerate(df_reader):
            yield idx, df
    except FileNotFoundError as e:
        logger.error(f"File not found: {csv_path}. Error: {e}", exc_info=True)
        raise FileNotFoundError(f"File not found: {csv_path}") from e
    except pd.errors.ParserError as e:
        logger.error(f"Unable to parse data from {csv_path} to dataframe: {e}", exc_info=True)
        raise pd.errors.ParserError(f"Unable to parse data to dataframe: {e}") from e
    except Exception as e:
        logger.critical(f"An unhandled exception occurred while reading data from {csv_path}: {e}", exc_info=True)
        raise Exception(
            f"An unhandled exception occurred while reading data: {e}"
        ) from e


def to_sql(ds_name: str, df: pd.DataFrame, conn_url: str) -> bool:
    """
    Loads a Pandas DataFrame into a SQL database table.

    Args:
        ds_name: The name of the dataset/table to load data into.
        df: The Pandas DataFrame to be loaded.
        conn_url: The database connection URL.

    Returns:
        True if the data is successfully loaded.
    """
    try:
        df.to_sql(ds_name, con=conn_url, if_exists="append", index=False)
        return True
    except pd.errors.DatabaseError as e:
        logger.error(f"Error loading data to database table '{ds_name}'. Error: {e}", exc_info=True)
        raise pd.errors.DatabaseError(
            f"Error loading data to database: {e}"
            ) from e

def get_column_names(
    schemas: dict, ds_name: str, sorting_key: str = "column_position"
) -> list:
    """
    Extracts and sorts column names for a specific dataset from the provided schemas.

    Args:
        schemas: A dictionary containing all dataset schemas.
        ds_name: The name of the dataset.
        sorting_key: The key to use for sorting columns (e.g., 'column_position').

    Returns:
        A list of sorted column names.
    """
    try:
        sorted_schema = sorted(schemas[ds_name], key=lambda x: x[sorting_key])
        return [col_detail["column_name"] for col_detail in sorted_schema]
    except KeyError as e:
        logger.error(f"Malformed schema for dataset '{ds_name}': either dataset, sorting key '{sorting_key}' or 'column name' are missing. Error: {e}", exc_info=True)
        raise e


if __name__ == "__main__":
    parser = ArgumentParser(
        description="pass ds_name and chunk size runtime arguments to file to db loader"
    )
    parser.add_argument(
        "-ds_name",
        nargs='+',
        default=None,
        help="Dataset name: e.g. 'orders' or 'orders', 'order_items'.",
        type=str
    )
    parser.add_argument(
        "-cs", 
        default=10000,
        help="Data chunk size: int e.g. 10000",
        type=int
    )
    args = parser.parse_args()
    ds_names = args.ds_name
    
    try:
        main(ds_names, args.cs)
    except Exception as e:
        logger.critical(f"Application terminated due to unhandled error: {e}", exc_info=True)