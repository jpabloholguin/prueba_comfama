import glob
import json
import logging
import os.path
import pandas as pd
import shutil
from sqlalchemy import create_engine
import psycopg2


CONFIG_FILE = "src/3_pipeline/config.json"


if not os.path.exists(CONFIG_FILE):
    raise FileNotFoundError(f"File {CONFIG_FILE} not found")

with open(CONFIG_FILE) as f:
    config = json.load(f)

logging.basicConfig(
    filename=os.path.join(config["logs_dir"], config["pipeline_log"]),
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
)


def extract(config):
    """Extracts data from csv files"""

    logging.info("Starting data extraction")
    dataframes = {}
    csv_files = glob.glob(os.path.join(config["stagging_dir"], "*.csv"))
    for file_path in csv_files:
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        try:
            dataframes[table_name] = pd.read_csv(file_path, sep=';', encoding='utf-8')
        except UnicodeDecodeError:
            logging.warning(f"Failed to read {file_path} with utf-8 encoding, trying with latin1")
            dataframes[table_name] = pd.read_csv(file_path, sep=';', encoding='latin1')
    logging.info("Data extraction completed")
    return dataframes


def transform(df, table_name):
    """Transforms data to be ready for loading"""

    logging.info("Starting data transformation")

    if table_name == "territorio_1":
        def corregir_valor(valor):
            """Corrige el formato del valor si tiene separadores de miles"""
            if valor.count('.') > 2:  # Si hay más de dos puntos, se asume que son separadores de miles
                return valor.replace('.', '')  # Eliminar todos los puntos
            return valor.replace('.', ',')  # Reemplazar punto por coma

        if 'LongitudDireccion' in df.columns:
            df['Longitud'] = df['LongitudDireccion'].astype(str).apply(corregir_valor)
            del df['LongitudDireccion']  # Elimina la columna antigua

        if 'LatitudDireccion' in df.columns:
            df['Latitud'] = df['LatitudDireccion'].astype(str).apply(corregir_valor)
            del df['LatitudDireccion']  # Elimina la columna antigua

    logging.info("Data transformation completed")
    return df




def load_to_postgres(df, table_name, engine):
    """Loads data to PostgreSQL"""

    logging.info(f"Loading data to table {table_name}")
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    logging.info(f"Data loaded to table {table_name}")


def etl():
    """Orchestrates ETL process"""

    logging.info("Starting ETL process")
    dataframes = extract(config)
    engine = create_engine(f"postgresql://{config['database_user']}:{config['database_password']}@{config['database_host']}:{config['database_port']}/{config['database_name']}")

    for table_name, df in dataframes.items():
        df = transform(df, table_name)
        load_to_postgres(df, table_name, engine)

    logging.info("ETL process completed")


def move_files():
    """Moves files from staging to ingested folder"""

    logging.info("Starting file moving")
    csv_files = glob.glob(os.path.join(config["stagging_dir"], "*.csv"))
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        target = os.path.join(config["ingested_dir"], file_name)
        shutil.move(file_path, target)
    logging.info("File moving completed")


if __name__ == "__main__":
    etl()
    move_files()
