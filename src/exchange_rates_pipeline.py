import io
import os
import requests
import zipfile
import pandas as pd
import psycopg2

from datetime import datetime
from pathlib import Path
from dotenv import dotenv_values
from psycopg2.extras import execute_values
from typing import Optional

from logger import logger

URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref.zip?0eb5bbd3afa62ca5cb9e7bd516a160fd"
PUBLIC_IP = dotenv_values(Path(".env"))["public_ip"]
PORT = dotenv_values(Path(".env"))["port"]
DB_NAME = dotenv_values(Path(".env"))["db_name"]
USER_NAME = dotenv_values(Path(".env"))["user_name"]
PASSWORD = dotenv_values(Path(".env"))["password"]


def connect_to_db(
    public_ip: str = PUBLIC_IP,
    port: int = PORT,
    db_name: str = DB_NAME,
    user_name: str = USER_NAME,
    password: str = PASSWORD,
):
    try:
        return psycopg2.connect(
            host=public_ip,
            database=db_name,
            user=user_name,
            password=password,
        )
    except psycopg2.Error as e:
        logger.error(f"Error connecting to the database: {str(e)}")
        raise


def read_sql_file(file_path: Path) -> str:
    """Read SQL query from file."""
    try:
        logger.info(f"Reading file '{file_path}'")
        return file_path.read_text()
    except FileNotFoundError:
        logger.error(f"Error: SQL file not found in '{file_path}'")
        raise


def update_schemas(
    sql_file: str,
    base_path: Optional[Path] = Path("sql/schemas/"),
) -> None:
    """
    Create or update table schemas before inserting data.
    """
    file_path = base_path / sql_file
    query = read_sql_file(file_path)

    try:
        with connect_to_db() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            logger.info(f"Successfully updated schema from file '{sql_file}'")
            return True
    except psycopg2.Error as e:
        logger.error(f"Error running query file {file_path}: {str(e)}")
        raise


def import_exchange_rate_from_csv_zip(
    url: str = URL,
    export_base_path: Path = Path("data/exchange_rates/"),
) -> dict:
    """
    Import the latest exchange rate from the ECB data portal.
    """

    try:
        response = requests.get(url)
        response.raise_for_status()

        data_zip = io.BytesIO(response.content)

        with zipfile.ZipFile(data_zip) as zip_file:
            for file_info in zip_file.infolist():
                data_csv = file_info.filename

            with zip_file.open(data_csv) as csv_file:
                df = pd.read_csv(csv_file)

                # Clean up
                df.columns = [
                    col.strip() if isinstance(col, str) else f"column_{i}"
                    for i, col in enumerate(df.columns)
                ]

                # Show info about the data
                date_col = df.columns[0]
                logger.info(f"Extracted rates for date: {df[date_col].iloc[0]}")

                # Process rates
                rates = dict()
                for currency in df.columns[1:]:
                    # Filter empty currency code
                    if not currency or (
                        isinstance(currency, str) and currency.isspace()
                    ):
                        continue
                    # Filter empty currency value
                    value = df[currency].iloc[0]
                    if pd.isna(value) or (isinstance(value, str) and not value.strip()):
                        logger.warn(f"Warning: Missing value for {currency}, skipping")

                    clean_currency = (
                        currency.strip() if isinstance(currency, str) else currency
                    )
                    rates[clean_currency] = float(value)

                # Insert missing EUR rate
                rates["EUR"] = 1.0

                # Add metadata
                df["updated_at"] = datetime.now()

                # Save CSV file
                today = datetime.today().date().isoformat()
                export_file_name = f"eurofxref-{today}.csv"
                full_export_path = export_base_path / export_file_name

                if not os.path.exists(export_base_path):
                    os.makedirs(export_base_path)
                df.to_csv(full_export_path, index=False, header=True)

                logger.info(f"Successfully retrieved {len(rates)} currency rates")
                return rates
    except requests.RequestException as e:
        logger.error(f"Error downloading exchange rates CSV zip: {str(e)}")
        raise
    except zipfile.BadZipFile as e:
        logger.error(f"Error with the zip file format: {str(e)}")
        raise


def write_latest_exchange_rates(rates: dict, table_name: str) -> None:
    """
    Insert exchange rates into table.
    """
    try:
        data = [
            (currency_code, rate, datetime.now())
            for currency_code, rate in rates.items()
        ]

        with connect_to_db() as conn:
            cursor = conn.cursor()
            execute_values(
                cursor,
                f"""
                INSERT INTO {table_name} (currency_code, rate, updated_at)
                VALUES %s
                ON CONFLICT (currency_code)
                DO UPDATE SET rate = EXCLUDED.rate, updated_at = EXCLUDED.updated_at
                """,
                data,
            )
            conn.commit()
            logger.info(f"Successfully wrote {len(data)} values into {table_name}")
    except psycopg2.Error as e:
        logger.info(f"Error saving data to database: {str(e)}")
        raise


def convert_order_details_currency():
    """Convert order details and append a new column with the converted currency."""
    try:
        with connect_to_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE orders o
                SET converted_amount_eur = 
                    CASE 
                        WHEN o.currency_code = 'EUR' THEN (o.revenue - o.order_discount)
                        ELSE (o.revenue - o.order_discount) / er.rate
                    END
                FROM ecb_exchange_rates er
                WHERE o.currency_code = er.currency_code
                """
            )
            conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Failed to convert orders: {str(e)}")
        raise


def main():
    logger.info("Extracting latest rates from ECB website ...")
    rates = import_exchange_rate_from_csv_zip()
    logger.info(f"Retrieved the following rates: {rates}")
    logger.info("Setting up schema ...")
    update_schemas(sql_file="ecb_exchange_rates.sql")
    logger.info("Writing rates to table ...")
    write_latest_exchange_rates(rates=rates, table_name="ecb_exchange_rates")
    logger.info("Update schema before converting ...")
    update_schemas(sql_file="add_converted_amount_column_to_orders.sql")
    logger.info("Converting order values to EUR ...")
    convert_order_details_currency()
    logger.info("Done!")


if __name__ == "__main__":
    main()
