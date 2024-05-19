import pandas as pd
from sqlalchemy import create_engine

olpa_engine = create_engine('mysql+pymysql://username:password@host/olpa_db')

datamart_engine = create_engine('mysql+pymysql://username:password@host/datamart_db')

customers_df = pd.read_sql('SELECT * FROM Customers', olpa_engine)
products_df = pd.read_sql('SELECT * FROM Product', olpa_engine)
product_subcategories_df = pd.read_sql('SELECT * FROM ProductSubCategory', olpa_engine)
product_categories_df = pd.read_sql('SELECT * FROM ProductCategory', olpa_engine)
order_header_df = pd.read_sql('SELECT * FROM OrderHeader', olpa_engine)
order_detail_df = pd.read_sql('SELECT * FROM OrderDetail', olpa_engine)

dim_customer_df = customers_df.copy()
dim_customer_df['CustomerFullName'] = dim_customer_df['FirstName'] + ' ' + dim_customer_df['MiddleName'].fillna('') + ' ' + dim_customer_df['LastName']
dim_customer_df['CustomerFullName'] = dim_customer_df['CustomerFullName'].str.replace('  ', ' ').str.strip()
dim_customer_df.to_sql('DimCustomer', datamart_engine, if_exists='replace', index=False)

dim_product_df = products_df.copy()
dim_product_df = dim_product_df.rename(columns={'name': 'ProductName'})
dim_product_df.to_sql('DimProduct', datamart_engine, if_exists='replace', index=False)

dim_product_subcategory_df = product_subcategories_df.copy()
dim_product_subcategory_df.to_sql('DimProductSubCategory', datamart_engine, if_exists='replace', index=False)

dim_product_category_df = product_categories_df.copy()
dim_product_category_df = dim_product_category_df.rename(columns={'Name': 'ProductCategoryName'})
dim_product_category_df.to_sql('DimProductCategory', datamart_engine, if_exists='replace', index=False)

def create_dim_date(start_date, end_date):
    date_range = pd.date_range(start_date, end_date)
    dim_date_df = pd.DataFrame({
        'DateID': date_range.strftime('%Y%m%d').astype(int),
        'Date': date_range,
        'Year': date_range.year,
        'Month': date_range.month,
        'Day': date_range.day,
        'Quarter': date_range.quarter
    })
    return dim_date_df

dim_date_df = create_dim_date('2000-01-01', '2024-12-31')
dim_date_df.to_sql('DimDate', datamart_engine, if_exists='replace', index=False)

fact_order_df = order_header_df.copy()
fact_order_df = fact_order_df.rename(columns={'OrderDate': 'OrderDateID', 'ShipDate': 'ShipDateID'})
fact_order_df['OrderDateID'] = pd.to_datetime(fact_order_df['OrderDateID']).dt.strftime('%Y%m%d').astype(int)
fact_order_df['ShipDateID'] = pd.to_datetime(fact_order_df['ShipDateID']).dt.strftime('%Y%m%d').astype(int)
fact_order_df.to_sql('FactOrder', datamart_engine, if_exists='replace', index=False)

fact_order_details_df = order_detail_df.copy()
fact_order_details_df = fact_order_details_df.merge(dim_product_df[['ProductID', 'ProductSubcategoryID']], on='ProductID', how='left')
fact_order_details_df = fact_order_details_df.merge(dim_product_subcategory_df[['ProductSubcategoryID', 'ProductCategoryID']], on='ProductSubcategoryID', how='left')
fact_order_details_df = fact_order_details_df.merge(dim_product_category_df[['ProductCategoryID', 'ProductCategoryName']], on='ProductCategoryID', how='left')
fact_order_details_df.to_sql('FactOrderDetails', datamart_engine, if_exists='replace', index=False)
