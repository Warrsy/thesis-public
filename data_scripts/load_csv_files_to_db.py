import csv
import psycopg2
from io import StringIO
import glob
import os

POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5432'
POSTGRES_DB = 'trinodb'
POSTGRES_USER = 'trino'
POSTGRES_PASSWORD = 'secret'
CSV_DELIMITER = ','  # Adjust if needed
CSV_HAS_HEADER = True  # Set to True if your CSV has a header row


def main():
    conn = None
    
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )

        conn.autocommit = False
        print("Connected to PostgreSQL database.")

        for model_num in range(0, 16):
            pattern = f"data/models/model{model_num}/model{model_num}_*.csv"
            matching_files = glob.glob(pattern)
            
            
            for file_path in matching_files:
                base_name = os.path.splitext(os.path.basename(file_path))[0]
                load_model_to_database(conn, file_path, base_name)

    except psycopg2.Error as e:
        print(f"Error connecting to or interacting with PostgreSQL: {e}")
        if conn:
            conn.rollback()
            print("Transaction rolled back due to error.")
    
    finally:
        if conn:
            conn.close()
            print("PostgreSQL connection closed.")


def create_table(conn, table_name, column_names):
    cursor = conn.cursor()
    columns = ', '.join([f"{name} TEXT" for name in column_names])

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns}
    );
    """

    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    print(f"Table {table_name} created successfully.")


def copy_data_from_csv(conn, table_name, csv_file_path, delimiter, has_header):
    cursor = conn.cursor()

    with open(csv_file_path, 'r') as csv_file:
        if has_header:
            next(csv_file)  # Skip the header row

        data = StringIO(csv_file.read())

        try: 
            cursor.copy_from(data, table_name, sep=delimiter, null='')
            conn.commit()
            print(f"Data from {csv_file_path} loaded into {table_name} successfully.")

        except Exception as e:
            print(f"Error loading data: {e}")
            conn.rollback()
    
    cursor.close()


def load_model_to_database(conn, file_path, table_name):
    with open(file_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=CSV_DELIMITER)
        
        try:
            header = next(csv_reader) if CSV_HAS_HEADER else None
        except StopIteration:
            header = None

    if header:
        create_table(conn, table_name, header)
    else:
        print("Warning: No header row found in CSV. Ensure your table schema matches.")

    copy_data_from_csv(conn, table_name, file_path, CSV_DELIMITER, CSV_HAS_HEADER)
    conn.commit()

if __name__ == "__main__":
    main()