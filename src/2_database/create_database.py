import json
import logging
import os.path
import psycopg2

CONFIG_FILE = "src/2_database/config.json"
SQL_SCRIPT = "src/2_database/create_tables.sql"  

if not os.path.exists(CONFIG_FILE):
    raise FileNotFoundError(f"File {CONFIG_FILE} not found")

with open(CONFIG_FILE) as f:
    config = json.load(f)

logging.basicConfig(
    filename=os.path.join(config["logs_dir"], config["create_database_log"]),
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
)

def create_database():
    """Crea la base de datos en PostgreSQL."""
    
    conn = psycopg2.connect(
        dbname=config["database_name"],
        user=config["database_user"],
        password=config["database_password"],
        host=config["database_host"],
        port=config["database_port"]
    )
    conn.close()


def load_sql_script():
    """Carga el script SQL para crear las tablas."""
    
    logging.info("Reading script for creating tables")
    if not os.path.exists(SQL_SCRIPT):
        raise FileNotFoundError(f"File {SQL_SCRIPT} not found")
    with open(SQL_SCRIPT, encoding="utf-8") as file:
        sql_script = file.read()
    return sql_script


def create_tables(sql_script):
    """Crea las tablas en PostgreSQL."""
    
    logging.info("Starting tables creation")
    conn = psycopg2.connect(
        dbname=config["database_name"],
        user=config["database_user"],
        password=config["database_password"],
        host=config["database_host"],
        port=config["database_port"]
    )
    cur = conn.cursor()
    cur.execute(sql_script)
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Tables creation completed")

def main():
    """Orquesta la creación de la base de datos y las tablas."""
    
    create_database()
    sql_script = load_sql_script()
    create_tables(sql_script)

if __name__ == "__main__":
    main()
