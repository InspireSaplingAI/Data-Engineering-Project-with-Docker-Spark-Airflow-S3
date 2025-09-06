#!/usr/bin/env python3
"""
Brazilian E-commerce Shipping Analysis with Spark
Identifying Late Deliveries Using Distributed Computing

This script uses Apache Spark to analyze Brazilian e-commerce data 
and identify orders where sellers missed shipping deadlines.
"""

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
import os
import zipfile

def main():
    """
    Main function to execute the Spark analysis pipeline
    """

    # Step 2: Initialize Spark Session
    print("\nStep 2: Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("Brazilian E-commerce Shipping Analysis") \
        .getOrCreate()

    # Step 3: Extract data from zip and load into Spark DataFrames
    print("\nStep 3: Extracting data from zip file...")
    # Get the parent directory of the current script
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    data_dir = os.path.join(base_dir, 'data')
    zip_path = os.path.join(data_dir, 'ecommerce_zipped_raw_data.zip')

    # Extract all contents of the zip file directly into the /data directory
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(data_dir)
    print(f"Extracted zip file to {data_dir}")

    # Update these paths to your actual CSV files (now directly in /data)
    items_path = os.path.join(data_dir, "olist_order_items_dataset.csv")
    orders_path = os.path.join(data_dir, "olist_orders_dataset.csv")
    products_path = os.path.join(data_dir, "olist_products_dataset.csv")

    df_items = spark.read.csv(items_path, header=True, inferSchema=True)
    df_orders = spark.read.csv(orders_path, header=True, inferSchema=True)
    df_products = spark.read.csv(products_path, header=True, inferSchema=True)

    print("DataFrames loaded successfully!")
    print(f"Items DataFrame shape: ({df_items.count()}, {len(df_items.columns)})")
    print(f"Orders DataFrame shape: ({df_orders.count()}, {len(df_orders.columns)})")
    print(f"Products DataFrame shape: ({df_products.count()}, {len(df_products.columns)})")

    # Step 4: Explore the Data
    print("\nStep 4: Exploring the data...")

    print("Sample records from Order Items:")
    df_items.show(5, truncate=False)

    print("\nSample records from Orders:")
    df_orders.show(5, truncate=False)

    print("\nSample records from Products:")
    df_products.show(5, truncate=False)

    # Check for null values
    print("\nNull values in Items DataFrame:")
    for col in df_items.columns:
        null_count = df_items.filter(df_items[col].isNull()).count()
        print(f"{col}: {null_count} null values")

    print("\nNull values in Orders DataFrame:")
    for col in df_orders.columns:
        null_count = df_orders.filter(df_orders[col].isNull()).count()
        print(f"{col}: {null_count} null values")

    # Step 5: Create SQL Views for Querying
    print("\nStep 5: Creating SQL views...")
    df_items.createOrReplaceTempView("items")
    df_orders.createOrReplaceTempView("orders")
    df_products.createOrReplaceTempView("products")
    print("Temporary views created successfully!")

    # Step 6: Data Preparation - Date Conversion
    print("\nStep 6: Converting date columns...")
    df_orders = df_orders.withColumn(
        "order_delivered_carrier_date",
        to_timestamp("order_delivered_carrier_date")
    ).withColumn(
        "order_approved_at",
        to_timestamp("order_approved_at")
    ).withColumn(
        "order_purchase_timestamp",
        to_timestamp("order_purchase_timestamp")
    )
    print("Date columns converted to timestamp format!")

    # Step 7: Execute Main Analysis Query
    print("\nStep 7: Executing main analysis query...")
    # Find orders where carrier delivered after shipping limit
    late_carrier_deliveries = spark.sql("""
        SELECT
            i.order_id,
            i.product_id,
            o.order_status,
            o.order_delivered_carrier_date,
            i.shipping_limit_date
        FROM items i
        JOIN orders o ON i.order_id = o.order_id
        WHERE to_timestamp(i.shipping_limit_date) < o.order_delivered_carrier_date
    """)
    print("Query executed successfully!")
    print(f"Number of late deliveries: {late_carrier_deliveries.count():,}")

    # Calculate percentage of late deliveries
    total_orders = df_orders.count()
    late_percentage = (late_carrier_deliveries.count() / total_orders) * 100 if total_orders > 0 else 0
    print(f"Total orders: {total_orders:,}")
    print(f"Late deliveries: {late_carrier_deliveries.count():,}")
    print(f"Percentage late: {late_percentage:.2f}%")

    # Step 8: Additional Analysis with Spark SQL
    print("\nStep 8: Performing additional analyses...")

    # Analysis 1: Late deliveries by product category
    print("Late deliveries by product category:")
    late_by_category = spark.sql("""
        SELECT
            p.product_category_name,
            COUNT(*) AS late_count
        FROM items i
        JOIN orders o ON i.order_id = o.order_id
        JOIN products p ON i.product_id = p.product_id
        WHERE to_timestamp(i.shipping_limit_date) < o.order_delivered_carrier_date
        GROUP BY p.product_category_name
        ORDER BY late_count DESC
    """)
    late_by_category.show(5, truncate=False)

    # Analysis 2: Late deliveries by order status
    print("\nLate deliveries by order status:")
    late_by_status = late_carrier_deliveries.groupBy("order_status").count()
    late_by_status.show(truncate=False)

    # Analysis 3: Calculate average delay time
    print("\nAverage delay time in hours:")
    from pyspark.sql.functions import unix_timestamp
    
    late_carrier_deliveries = late_carrier_deliveries.withColumn(
        "delay_hours",
        (unix_timestamp("order_delivered_carrier_date") - unix_timestamp(to_timestamp("shipping_limit_date"))) / 3600
    )
    avg_delay = late_carrier_deliveries.selectExpr("avg(delay_hours) as avg_delay_hours").collect()[0]["avg_delay_hours"]
    print(f"Average delay time: {avg_delay:.2f} hours")

    # Step 9: Save Results to Output File
    print("\nStep 9: Saving results...")
    output_path = "data/missed_shipping_limit_orders.csv"  # EDIT THIS PATH
    late_carrier_deliveries.write.csv(output_path, header=True, mode="overwrite")
    print(f"Results saved successfully to: {output_path}")

    # Step 10: Performance Optimization (Optional)
    print("\nStep 10: Applying performance optimizations...")
    df_items.cache()
    df_orders.cache()
    df_products.cache()
    print("DataFrames cached for better performance!")

    # Step 11: Cleanup and Session Management
    print("\nStep 11: Cleaning up resources...")
    df_items.unpersist()
    df_orders.unpersist()
    df_products.unpersist()
    print("Cached DataFrames unpersisted!")
    print("Analysis completed successfully! 🚀")

    # Summary
    print("\n" + "="*50)
    print("SUMMARY")
    print("="*50)
    print("✅ Successfully processed e-commerce data using Apache Spark")
    print("✅ Identified orders with missed shipping deadlines")
    print("✅ Performed distributed SQL queries on large datasets")
    print("✅ Exported results for further analysis")
    print(f"✅ Found {late_carrier_deliveries.count():,} late deliveries ({late_percentage:.2f}%)")
    print("="*50)

    spark.stop()

if __name__ == "__main__":
    main()