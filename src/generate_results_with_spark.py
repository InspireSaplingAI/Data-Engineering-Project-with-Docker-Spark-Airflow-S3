#!/usr/bin/env python3
"""
Brazilian E-commerce Shipping Analysis with Spark
Identifying Late Deliveries Using Distributed Computing

This script uses Apache Spark to analyze Brazilian e-commerce data 
and identify orders where sellers missed shipping deadlines.
"""

# Import required libraries
from zipfile import ZipFile
import findspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import to_timestamp, date_format
import os

def main():
    """
    Main function to execute the Spark analysis pipeline
    """
    
    # Step 1: Extract the Data Archive
    print("Step 1: Extracting data archive...")
    
    # Step 2: Initialize Spark Session
    print("\nStep 2: Initializing Spark session...")

    
    # Step 3: Load Data into Spark DataFrames
    print("\nStep 3: Loading data into Spark DataFrames...")
    
   
    
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
    
   
    print("Temporary views created successfully!")
    
    # Step 6: Data Preparation - Date Conversion
    print("\nStep 6: Converting date columns...")
   
    
    print("Date columns converted to timestamp format!")
    
    # Step 7: Execute Main Analysis Query
    print("\nStep 7: Executing main analysis query...")

    
    print("Query executed successfully!")
    print(f"Number of late deliveries: {late_carrier_deliveries.count():,}")
    
    # Calculate percentage of late deliveries
   
    print(f"Total orders: {total_orders:,}")
    print(f"Late deliveries: {late_carrier_deliveries.count():,}")
    print(f"Percentage late: {late_percentage:.2f}%")
    
    # Step 8: Additional Analysis with Spark SQL
    print("\nStep 8: Performing additional analyses...")
    
    # Analysis 1: Late deliveries by product category
    print("Late deliveries by product category:")
   
    # Analysis 2: Late deliveries by order status
    print("\nLate deliveries by order status:")
   
    
    # Analysis 3: Calculate average delay time
    print("\nAverage delay time in hours:")
  
    
    # Step 9: Save Results to Output File
    print("\nStep 9: Saving results...")
    
    output_path = "data/missed_shipping_limit_orders.csv"  # EDIT THIS PATH
    
    
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

if __name__ == "__main__":
    main()