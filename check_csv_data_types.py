import pandas as pd

# Đường dẫn đến các file CSV của bạn
csv_files = {
    'product': 'D:/Project/Data/product.csv',
    'transaction_data': 'D:/Project/Data/transaction_data.csv',
    'hh_demographic': 'D:/Project/Data/hh_demographic.csv',
    'campaign_table': 'D:/Project/Data/campaign_table.csv',
    'coupon': 'D:/Project/Data/coupon.csv',
    'campaign_desc': 'D:/Project/Data/campaign_desc.csv',
    'coupon_redempt': 'D:/Project/Data/coupon_redempt.csv'
}

# Định nghĩa các kiểu dữ liệu mà bạn đã tạo trong data warehouse
expected_data_types = {
    'product': {
        'PRODUCT_ID': int,
        'MANUFACTURER': int,
        'DEPARTMENT': str,
        'BRAND': str,
        'COMMODITY_DESC': str,
        'SUB_COMMODITY_DESC': str,
        'CURR_SIZE_OF_PRODUCT': str
    },
    'transaction_data': {
        'household_key': int,
        'BASKET_ID': int,
        'DAY': int,
        'PRODUCT_ID': int,
        'QUANTITY': int,
        'SALES_VALUE': float,
        'STORE_ID': int,
        'RETAIL_DISC': float,
        'TRANS_TIME': int,
        'WEEK_NO': int,
        'COUPON_DISC': int,
        'COUPON_MATCH_DISC': int,
        'DAY2': str
    },
    'hh_demographic': {
        'AGE_DESC': str,
        'MARITAL_STATUS_CODE': str,
        'INCOME_DESC': str,
        'HOMEOWNER_DESC': str,
        'HH_COMP_DESC': str,
        'HOUSEHOLD_SIZE_DESC': str,
        'KID_CATEGORY_DESC': str,
        'household_key': int
    },
    'campaign_table': {
        'DESCRIPTION': str,
        'household_key': int,
        'CAMPAIGN': int
    },
    'coupon': {
        'COUPON_UPC': int,
        'PRODUCT_ID': int,
        'CAMPAIGN': int
    },
    'campaign_desc': {
        'DESCRIPTION': str,
        'CAMPAIGN': int,
        'START_DAY': int,
        'END_DAY': int
    },
    'coupon_redempt': {
        'household_key': int,
        'DAY': int,
        'COUPON_UPC': int,
        'CAMPAIGN': int
    }
}

# Hàm để kiểm tra kiểu dữ liệu của từng cột trong mỗi file CSV
def check_csv_data_types(csv_file, expected_data_types):
    print(f"Checking data types for {csv_file}...")

    # Đọc file CSV vào DataFrame
    df = pd.read_csv(csv_file)

    # Lấy tên bảng từ tên file CSV
    table_name = csv_file.split('/')[-1].split('.')[0]

    # Kiểm tra từng cột trong DataFrame
    for column in df.columns:
        if column in expected_data_types[table_name]:
            expected_type = expected_data_types[table_name][column]
            actual_type = df[column].dtype

            if actual_type == expected_type:
                print(f"Column '{column}' has correct data type: {actual_type}")
            else:
                print(f"Column '{column}' has incorrect data type. Expected: {expected_type}, Actual: {actual_type}")

    print(f"Data type check for {table_name} complete.\n")

# Kiểm tra từng file CSV
for table_name, csv_file in csv_files.items():
    check_csv_data_types(csv_file, expected_data_types)
