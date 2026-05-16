"""
A Simple Data Processing Pipeline
Download data from S3, clean it, transform it, and upload results back to S3.
"""

import os
import boto3
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
import numpy as np
import tempfile


def process_ecommerce_data():
    """Download, process, and upload e-commerce data"""

    print("Starting data processing...")

    # Load environment variables
    load_dotenv()
    bucket_name = os.getenv('AWS_S3_BUCKET_NAME')
    region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

    if not bucket_name:
        print("ERROR: AWS_S3_BUCKET_NAME not found in .env file!")
        return False

    print(f"Processing data from bucket: {bucket_name}")

    try:
        # Create S3 client
        s3_client = boto3.client('s3', region_name=region)

        # Step 1: Download data from S3
        print("\nStep 1: Downloading data from S3...")
        datasets = download_data_from_s3(s3_client, bucket_name)

        # Step 2: Clean and transform data
        print("\nStep 2: Cleaning and transforming data...")
        processed_datasets = transform_data(datasets)

        # Step3: Business Metrics
        print("\nStep3: Creating buisness metrics...")
        business_metrics = create_business_metrics(processed_datasets)

        # Step 4: Upload processed data back to S3
        print("\nStep 4: Uploading processed data to S3...")
        upload_success = upload_processed_data(s3_client, bucket_name, processed_datasets)

        if upload_success:
            print("\nSUCCESS: Data processing pipeline completed!")
            return True
        else:
            print("\nERROR: Failed to upload processed data")
            return False

    except Exception as e:
        print(f"ERROR: Data processing failed: {e}")
        return False


def download_data_from_s3(s3_client, bucket_name):
    dataset_dict = {}
    # data_files = os.listdir("../data") #<--- broken
    # data_files = os.listdir("../../data")
    data_files = ['customers.csv', 'order_items.csv', 'orders.csv', 'products.csv', 'reviews.csv']

    for f_name in data_files:
        try:
            print(f"Downloading {f_name}...")
            local_path = os.path.join("../../Download", f_name)

            s3_client.download_file(bucket_name, f"data/{f_name}", local_path)
            df = pd.read_csv(local_path)
            dict_key = f_name.replace(".csv", "")
            dataset_dict[dict_key] = df

            print(f"[SUCCESS] {dict_key} has been loaded with records: {len(df)}\n")
            os.remove(local_path)

        except Exception as e:
            print(f"Failed to download {f_name} with: {e}\n")
    return dataset_dict


def transform_data(datasets):
    processed = {}

    # "customers" pre-processing
    if 'customers' in datasets:
        customers = datasets['customers'].copy()

        # Cleaning email address
        customers['email'] = customers['email'].str.lower().str.strip()

        # Converting dates into datetime datatype
        customers['date_of_birth'] = pd.to_datetime(customers['date_of_birth'])
        customers['registration_date'] = pd.to_datetime(customers['registration_date'])

        # Feature enhancement
        ## adding 'age' column
        customers['age'] = (datetime.now() - customers['date_of_birth']).dt.days // 365

        ## adding 'age_group' column
        customers['age_group'] = pd.cut(customers['age'],
                                        bins=np.linspace(0, 100, 6),  # [  0.,  20.,  40.,  60.,  80., 100.]
                                        labels=['0-20', '21-40', '41-60', '61-80', '80+'])
        processed['cleaned_customers'] = customers
        print(f"\"Customers\" pre-processing completed with shape: {customers.shape}\n")

    # "products" pre-processing
    if "products" in datasets:
        products = datasets['products'].copy()

        # Addressing product_name
        products['product_name'] = products['product_name'].str.strip()

        # Converting "created_date" and "last_updated" to the datatype - datetime
        products['created_date'] = pd.to_datetime(products['created_date'])
        products['last_updated'] = pd.to_datetime(products['last_updated'])

        # Converting products['price'] to numeric (seems redundant to me)
        products['price'] = pd.to_numeric(products['price'], errors='coerce')

        # Creating price groups
        products['price_category'] = pd.cut(products['price'],
                                            bins=np.linspace(np.min(products['price']), np.max(products['price']), 5),
                                            labels=['Budget', 'Medium', 'Premium', 'Luxury'])
        processed['cleaned_products'] = products
        print(f"\"Products\" pre-processing completed with shape: {products.shape}\n")

    # "orders" pre-processing
    if 'orders' in datasets:
        orders = datasets['orders'].copy()

        # Converting the data type of "date" to datetime
        orders["order_date"] = pd.to_datetime(orders["order_date"])

        # Converting "total_amount" to numeric (seems redundant to me)
        orders["total_amount"] = pd.to_numeric(orders["total_amount"])

        # Extracting month and year for seasonal analysis
        orders["order_year"] = orders["order_date"].dt.year
        orders["order_month"] = orders["order_date"].dt.month

        processed['cleaned_orders'] = orders
        print(f"\"Orders\" pre-processing completed with shape: {orders.shape}\n")

    # "order_items" pre-processing
    if 'order_items' in datasets:
        order_items = datasets['order_items'].copy()

        # Converting numerical columns (seems redundant to me)
        order_items['quantity'] = pd.to_numeric(order_items['quantity'], errors='coerce')
        order_items['unit_price'] = pd.to_numeric(order_items['unit_price'], errors='coerce')
        order_items['discount_amount'] = pd.to_numeric(order_items['discount_amount'], errors='coerce')

        # Calculating total price of each item
        order_items['total_price'] = order_items['unit_price'] * order_items['quantity']
        processed['cleaned_order_items'] = order_items
        print(f"\"Order_items\" pre-processing completed with shape: {order_items.shape}\n")

    # "reviews" pre-processing
    if 'reviews' in datasets:
        reviews = datasets['reviews'].copy()

        # Converting 'review_date' to datetime
        reviews['review_date'] = pd.to_datetime(reviews['review_date'])

        # Convert 'rating' to numeric
        reviews['rating'] = pd.to_numeric(reviews['rating'], errors='coerce')

        # Converting 'rating' to categorical data
        reviews['rating_category'] = reviews['rating'].apply(lambda x: 'Excellent' if x >= 4.5 else
        'Good' if x >= 3.5 else
        'Average' if x >= 2.5 else
        'Poor')

        processed['cleaned_reviews'] = reviews
        print(f'Processed reviews: {reviews.shape} records')

    return processed


def create_business_metrics(processed_datasets):
    metrics = {}

    # Customer metric
    if 'cleaned_customers' in processed_datasets and 'cleaned_orders' in processed_datasets:
        cleaned_customers = processed_datasets['cleaned_customers']
        cleaned_orders = processed_datasets['cleaned_orders']

        # Customer lifetime value calculation
        customer_metrics = cleaned_orders.groupby('customer_id').agg({'total_amount': ['sum', 'count', 'mean'],
                                                                      # Calculating "total_spent", "number of orders", and "average amount per order" of each customer_id
                                                                      'order_date': ['min', 'max']}).round(2)
        customer_metrics.columns = ['total_spent', 'order_count', 'avg_order_price', 'first_order', 'last_order']
        customer_metrics = customer_metrics.reset_index()
        customer_metrics = customer_metrics.merge(cleaned_customers[['customer_id', 'age_group']],
                                                  on='customer_id')  # 把兩個 DataFrame 用 customer_id 對齊後合併)
        metrics['customer_metrics'] = customer_metrics
        print(f"Created customer metrics: {len(customer_metrics)} customers")

        # Product Performance metrics
        if 'cleaned_products' in processed_datasets and 'cleaned_order_items' in processed_datasets:
            cleaned_products = processed_datasets['cleaned_products']
            cleaned_order_items = processed_datasets['cleaned_order_items']

            # Product Performance Evaluation
            product_metrics = cleaned_order_items.groupby('product_id').agg(
                {'quantity': 'sum',  # 計算這項產品總共賣出多少件 Calculating the quantity of the product sold
                 'total_price': 'sum',  # 計算這項產品的總收益 Calculating the total revenue of the product
                 'order_id': 'count'  # 計算這項產品有多少訂單 Calculating the number of orders of the product
                 }).round(2)
            product_metrics.columns = ['total_quantity_sold', 'total_revenue', 'number_of_orders']
            product_metrics = product_metrics.reset_index()
            product_metrics = product_metrics.merge(
                cleaned_products[['product_id', 'product_name', 'category', 'price']],
                on='product_id')  # 參數on: 把兩個 DataFrame 用 product_id 對齊後合併
            metrics['product_metrics'] = product_metrics
            print(f"Created product metrics: {len(product_metrics)} products")

        # Sales Revenue metrics
        if 'cleaned_orders' in processed_datasets:
            cleaned_orders = processed_datasets['cleaned_orders']
            sales_revenue_metrics = cleaned_orders.groupby(['order_year', 'order_month']).agg(
                {'total_amount': 'sum',  # monthly revenue performance
                 'order_id': 'count'}).round(2)  # monthly order counts (workload)
            sales_revenue_metrics.columns = ['total_revenue', 'order_count']
            sales_revenue_metrics = sales_revenue_metrics.reset_index()

            metrics['sales_revenue_metrics'] = sales_revenue_metrics
            print(f"Created monthly sales revenue metrics: {len(sales_revenue_metrics)} months")

        return metrics


def upload_processed_data(s3_client, bucket_name, processed):
    upload_count = 0
    total_files = len(processed)  # + len(metrics)

    for dataset_name, df in processed.items():
        try:
            local_path = os.path.join(tempfile.gettempdir(), f"{dataset_name}.csv")
            df.to_csv(local_path, index=False)

            # Uploading to S3
            s3_key = f"processed/{dataset_name}.csv"
            s3_client.upload_file(local_path, bucket_name, s3_key)
            # s3_client.upload_file(local_path, xxx, s3_key) # BROKEN
            print(f"Uploaded {dataset_name} with shape {df.shape}\n")

            upload_count += 1

            # Clean up
            os.remove(local_path)

        except Exception as e:
            print(f"Failed to upload {dataset_name}.\n[ERROR]: {e}\n")

    return upload_count == total_files


if __name__ == "__main__":
    success = process_ecommerce_data()
    if success:
        print("\nNext step: Orchestration with Prefect!")
    else:
        print("\nFix the errors")
