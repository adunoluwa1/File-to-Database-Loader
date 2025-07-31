# CSV to Database Loader

A Python script for efficiently ingesting large CSV files into a PostgreSQL database. This tool is designed to be robust and configurable, handling data in manageable chunks to prevent memory issues and using a declarative schema to ensure data integrity.

## Features

* **Chunk-based Processing:** Reads and loads large CSV files in configurable chunks to optimize memory usage.

* **Declarative Schema:** Uses an external `schemas.json` file to define column names and positions, ensuring data is loaded correctly even without headers.

* **Multi-Dataset Support:** Can load one or more datasets from a single command-line execution.

* **Robust Error Handling:** Includes explicit checks for missing environment variables, file existence, and parsing errors.

* **Environment-based Configuration:** Uses environment variables for secure management of sensitive database credentials.

* **Argument Parsing:** A user-friendly command-line interface for specifying datasets and chunk size.

## Prerequisites

Before running the script, ensure you have the following installed:

* Python 3.x

* PostgreSQL database

* The required Python libraries.

You can install the Python dependencies using pip:

`pip install pandas psycopg2-binary`

> **Note:** `psycopg2-binary` is required for pandas to connect to a PostgreSQL database.

## Project Structure

The project expects the following file and directory structure:

```
├── app.py                  # The main script
├── schemas.json            # Configuration file for dataset schemas
└── data/                   # Directory containing your data
├── orders/
│   ├── part-00000.csv
│   └── part-00001.csv
└── order_items/
├── part-00000.csv
└── part-00001.csv
```
## `schemas.json` Example

The `schemas.json` file defines the structure for each dataset. The `column_position` key is crucial for sorting the columns correctly.
```
{
"orders": [
{ "column_name": "order_id", "column_position": 1 },
{ "column_name": "order_date", "column_position": 2 },
{ "column_name": "customer_id", "column_position": 3 },
{ "column_name": "order_status", "column_position": 4 }
],
"order_items": [
{ "column_name": "order_item_id", "column_position": 1 },
{ "column_name": "order_item_order_id", "column_position": 2 },
{ "column_name": "order_item_product_id", "column_position": 3 },
{ "column_name": "order_item_quantity", "column_position": 4 }
]
}
```


## Usage

### 1. Set Environment Variables

First, set the environment variables required for the database connection and the source data directory.

```
export SRC_BASE_DIR="/path/to/your/data"

export DB_USER="your_db_user"

export DB_PASS="your_db_password"

export DB_HOST="your_db_host"

export DB_PORT="5432"

export DB_NAME="your_db_name"
```

### 2. Run the Script

The script uses command-line arguments to specify the dataset(s) to load and the chunk size.

* `-ds_name`: The name(s) of the dataset(s) to load. Can be a single name or multiple names separated by a space.

* `-cs`: The chunk size (number of rows) to process at a time. Defaults to `10000`.

**To load a single dataset:**

`python app.py -ds_name orders -cs 10000`


**To load multiple datasets:**

`python app.py -ds_name orders order_items -cs 5000`


## Design Decisions

* **Robust Configuration:** The script prioritizes secure and explicit configuration by using environment variables. It includes a validation step to ensure all necessary variables are present before execution.

* **`argparse` for CLI:** The `argparse` module is used for its robust and user-friendly command-line interface. The `nargs='+'` parameter for `-ds_name` simplifies the input of multiple datasets.

* **Modular Functions:** The code is broken down into small, single-responsibility functions (`db_loader`, `read_csv`, `to_sql`, etc.) to enhance readability, testability, and maintainability.

* **Error Handling Strategy:** The script employs a "fail-fast" approach for critical issues (e.g., missing environment variables or schema files) but is more resilient during data loading, printing errors for failed chunks and continuing to the next.

## Contributing

Feel free to open an issue or submit a pull request if you have suggestions for improvements.

## License

This project is licensed under the MIT License.
