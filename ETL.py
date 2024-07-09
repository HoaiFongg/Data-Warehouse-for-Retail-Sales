import pandas as pd
import pyodbc

# SQL Server connection function
def connect_to_sql_server():
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=HOAIFONG\SQLEXPRESS;'
        'DATABASE=DataWarehouse-retail-sales;'
        'UID=notfong;'
        'PWD=11032002'
    )
    return conn

# Extract data from CSV files
def extract():
    dataframes = {}
    data_files = [
        'product.csv',
        'transaction_data.csv',
        'hh_demographic.csv',
        'campaign_table.csv',
        'coupon.csv',
        'campaign_desc.csv',
        'coupon_redempt.csv'
    ]
    base_path = 'D:/Project/Data/'  # Update with your actual base path
    
    for file in data_files:
        df = pd.read_csv(base_path + file)
        dataframes[file.split('.')[0]] = df
    
    return dataframes

# Transform data (if needed)
def transform(dataframes):
    # Perform any transformation required on the dataframes here
    # For now, we assume no transformation is needed and just return the dataframes as is
    return dataframes

# Load data into SQL Server tables
def load(conn, dataframes, batch_size=1000):
    cursor = conn.cursor()
    
    def insert_data(df, table_name, primary_key, foreign_keys=None):
        num_rows = df.shape[0]
        for start in range(0, num_rows, batch_size):
            end = min(start + batch_size, num_rows)
            batch_df = df.iloc[start:end]

            for index, row in batch_df.iterrows():
                # Convert numpy values to Python basic data types
                row = row.apply(lambda x: x.item() if hasattr(x, 'item') else x)
                
                # Check foreign key constraints if specified
                if foreign_keys:
                    foreign_key_violation = False
                    for fk, fk_table, fk_column in foreign_keys:
                        sql_check_fk = f"SELECT COUNT(*) FROM {fk_table} WHERE {fk_column} = ?"
                        cursor.execute(sql_check_fk, (int(row[fk]),))
                        if cursor.fetchone()[0] == 0:
                            foreign_key_violation = True
                            print(f"Foreign key constraint violation in {table_name}: {fk} = {row[fk]}")
                            break
                    
                    if foreign_key_violation:
                        continue
                
                placeholders = ', '.join(['?'] * len(row))
                columns = ', '.join(row.index)
                sql_check = f"SELECT COUNT(*) FROM {table_name} WHERE {primary_key} = ?"
                cursor.execute(sql_check, (int(row[primary_key]),))
                if cursor.fetchone()[0] == 0:
                    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    cursor.execute(sql, tuple(row))
                    print(f"Inserted row into {table_name}: {tuple(row)}")
                else:
                    print(f"Duplicate row found in {table_name}: {tuple(row)}")
            conn.commit()
    
    insert_data(dataframes['product'], 'product', 'PRODUCT_ID')
    insert_data(dataframes['hh_demographic'], 'hh_demographic', 'household_key')
    insert_data(dataframes['transaction_data'], 'transaction_data', 'BASKET_ID', [('household_key', 'hh_demographic', 'household_key'), ('PRODUCT_ID', 'product', 'PRODUCT_ID')])
    insert_data(dataframes['campaign_table'], 'campaign_table', 'household_key', [('household_key', 'hh_demographic', 'household_key'), ('CAMPAIGN', 'campaign_desc', 'CAMPAIGN')])
    insert_data(dataframes['coupon'], 'coupon', 'COUPON_UPC', [('PRODUCT_ID', 'product', 'PRODUCT_ID'), ('CAMPAIGN', 'campaign_desc', 'CAMPAIGN')])
    insert_data(dataframes['campaign_desc'], 'campaign_desc', 'CAMPAIGN')
    insert_data(dataframes['coupon_redempt'], 'coupon_redempt', 'COUPON_UPC', [('household_key', 'hh_demographic', 'household_key'), ('COUPON_UPC', 'coupon', 'COUPON_UPC')])
    
    cursor.close()

# Main ETL function
def etl():
    print("Starting ETL process...")
    conn = connect_to_sql_server()
    print("Connected to SQL Server.")
    
    print("Extracting data from CSV files...")
    dataframes = extract()
    print("------Data extracted successfully------")
    
    print("Transforming data...")
    transformed_dataframes = transform(dataframes)
    print("------Data transformed successfully------")
    
    print("Loading data into SQL Server...")
    load(conn, transformed_dataframes)
    print("------Data loaded successfully------")
    
    conn.close()
    print("------------ETL process completed------------")

# Entry point of the script
if __name__ == "__main__":
    etl()
