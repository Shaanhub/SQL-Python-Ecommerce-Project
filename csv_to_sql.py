import pandas as pd
import mysql.connector
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sales.csv', 'sales'),
    ('products.csv', 'products'),
    ('delivery.csv', 'delivery'),
    ('payments.csv', 'payments')  # Added payments.csv for specific handling
]

# Function to get SQL data type from pandas dtype
def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

# Batch size for insertions
batch_size = 1000

# Connect to the MySQL database
try:
    conn = mysql.connector.connect(
        host=os.getenv('DB_HOST', 'your_host'),  # Default to 'your_host' if env variable not set
        user=os.getenv('DB_USER', 'your_username'),
        password=os.getenv('DB_PASSWORD', 'your_password'),
        database=os.getenv('DB_NAME', 'your_database')
    )
    cursor = conn.cursor()
    logging.info("Database connection successful")
except mysql.connector.Error as err:
    logging.error(f"Error: {err}")
    exit(1)

# Folder containing the CSV files
folder_path = 'path_to_your_folder'

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)

    # Check if CSV exists
    if not os.path.exists(file_path):
        logging.warning(f"{csv_file} not found in the specified folder.")
        continue
    
    try:
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(file_path)
        logging.info(f"Processing {csv_file}")
        
        # Replace NaN with None to handle SQL NULL
        df = df.where(pd.notnull(df), None)
        
        # Clean column names
        df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

        # Check for missing columns (you can add your own checks here)
        if df.isnull().sum().any():
            logging.warning(f"Warning: {csv_file} contains missing data.")

        # Generate the CREATE TABLE statement with appropriate data types
        columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
        create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
        cursor.execute(create_table_query)

        # Batch insert rows into MySQL table
        rows = []
        for _, row in df.iterrows():
            values = tuple(None if pd.isna(x) else x for x in row)
            rows.append(values)

            # Insert in batches
            if len(rows) == batch_size:
                cursor.executemany(f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})", rows)
                conn.commit()
                logging.info(f"Inserted {len(rows)} rows into {table_name}")
                rows = []  # Reset rows after committing

        # Insert any remaining rows
        if rows:
            cursor.executemany(f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})", rows)
            conn.commit()
            logging.info(f"Inserted remaining {len(rows)} rows into {table_name}")

    except Exception as e:
        logging.error(f"Error processing {csv_file}: {e}")
        continue

# Close the database connection
cursor.close()
conn.close()
logging.info("Database connection closed")
