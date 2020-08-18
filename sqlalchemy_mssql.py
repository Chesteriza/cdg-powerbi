# -*- coding: utf-8 -*-
"""
Created on Wed Aug 12 21:45:40 2020

@author: chest
"""

import sqlalchemy as sal
import os
import pickle
import pandas as pd
import re

## Set working directory
os.chdir(r'C:\Users\chest\Desktop\Comfort Delgro')

## Establish database connection
conn = sal.create_engine("mssql+pyodbc://CHESTERIZA\SQLEXPRESS/cdg?driver=SQL Server?Trusted_Connection=yes")

## Function to execute SQL Query
def execute_query(conn, query):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    conn.execute(query)


## Creating respective tables based on scripts provided

table_1_sql = '''CREATE TABLE [dbo].FactStore
                (DateID date,
                 StoreID integer,
                 ProductID integer,
                 OnHandQty integer,
                 OnOrderQty integer,
                 DaysInStock integer,
                 MinDayInStock integer,
                 MaxDayInStock integer)'''

table_2_sql = '''CREATE TABLE [dbo].[DimDates](
            	[DateID] [datetime] NOT NULL,
            	[FullDateLabel] [nvarchar](20) NOT NULL,
            	[CalendarYear] [int] NOT NULL,
            	[CalendarQuarter] [int] NOT NULL,
            	[CalendarMonth] [int] NOT NULL,
            	[CalendarMonthName] [nvarchar](20) NOT NULL,
            	[CalendarDayOfWeek] [int] NOT NULL,
            	[WeekDayName] [nvarchar](10) NOT NULL,
            	[EuropeSeason] [nvarchar](50) NULL,
            	[NorthAmericaSeason] [nvarchar](50) NULL,
            	[AsiaSeason] [nvarchar](50) NULL) '''

table_3_sql = '''CREATE TABLE [dbo].[DimProducts](
            	[ProductID] [int] NOT NULL,
            	[ProductName] [nvarchar](500) NULL,
            	[ProductDescription] [nvarchar](400) NULL,
            	[ProductCategoryName] [nvarchar](30) NOT NULL,
            	[ProductSubcategoryName] [nvarchar](50) NOT NULL,
            	[Manufacturer] [nvarchar](50) NULL)'''

table_4_sql = '''CREATE TABLE [dbo].[DimStores](
            	[StoreID] [int] NOT NULL,
            	[StoreName] [nvarchar](100) NOT NULL,
            	[CityName] [nvarchar](100) NULL,
            	[StateProvinceName] [nvarchar](100) NULL,
            	[RegionCountryName] [nvarchar](100) NULL,
            	[StoreManager] [nvarchar](101) NOT NULL,
            	[StoreType] [nvarchar](15) NULL) '''

    
execute_query(conn, table_1_sql)
execute_query(conn, table_2_sql)
execute_query(conn, table_3_sql)
execute_query(conn, table_4_sql)
    
## Loading main_df that has been unpacked from files provided
with open("master_df.pkl", "rb") as file:
   df= pickle.load(file)

## Creating respective df and upload
##  Factstore Table
factstore_cols = ['Date',
                  'StoreID',
                  'ProductID',
                  'OnHandQuantity',
                  'OnOrderQuantity',
                  'DaysInStock',
                  'MinDayInStock',
                  'MaxDayInStock']

factstore_df = df[factstore_cols].copy()
factstore_sql_columns = pd.read_sql("SELECT * FROM FactStore", conn).columns.tolist()
col_map = dict(zip(factstore_cols, factstore_sql_columns))
factstore_df.rename(columns=col_map, inplace=True)

## Split the writing of DF to make it faster 
total_rows = 0 
for i in range(100000,1610000,100000):
    print(i)
    if i == 100000:
        subset_df = factstore_df[:i]
        subset_df.to_sql('FactStore', conn, if_exists="append", index=False)
        total_rows += subset_df.shape[0]
        print("current rows added: " + str(subset_df.shape[0]))
        print("total rows added: " + str(total_rows))
        prev = i
    elif i == 1600000:
        subset_df = factstore_df[prev+1:]
        subset_df.to_sql('FactStore', conn, if_exists="append", index=False)
        total_rows += subset_df.shape[0]
        print("current rows added: " + str(subset_df.shape[0]))
        print("total rows added: " + str(total_rows))
    else:
        subset_df = factstore_df[prev:i]
        subset_df.to_sql('FactStore', conn, if_exists="append", index=False)
        prev = i
        total_rows += subset_df.shape[0]
        print("current rows added: " + str(subset_df.shape[0]))
        print("total rows added: " + str(total_rows))

## Insert values into DimDates, DimProducts, DimStores
with open(r"sql_db\sample_sql.txt", "r", encoding="utf8") as file:
    sql_script = file.read()

## Split into list to process by batch with SQLAlchemy
sql_script_list = sql_script.split("GO")
sql_script_list = [i for i in sql_script_list if len(i) > 0]
for i in range(len(sql_script_list)):
    print(i)
    execute_query(conn, sql_script_list[i])

## Perform some data cleaning of the DimDates Table 
DimDates_df = pd.read_sql("SELECT * FROM DimDates", conn)
## Notice Quarter, Month and Day are not in proper format
DimDates_df.CalendarQuarter = DimDates_df.CalendarQuarter.astype(str)
DimDates_df.CalendarMonth = DimDates_df.CalendarMonth.astype(str)
DimDates_df.CalendarDayOfWeek = DimDates_df.CalendarDayOfWeek.astype(str)

## Perform Regex Lambda to get information 
DimDates_df.CalendarQuarter = DimDates_df.CalendarQuarter.apply(lambda x: re.match(r"2009(\d+)", x).group(1))
DimDates_df.CalendarMonth = DimDates_df.CalendarMonth.apply(lambda x: re.match(r"2009(\d*)", x).group(1))

## Due to issues with the original date of CalendarDayOfWeek, we will use datetime package to generate features of the Calendary DateTime
DimDates_df.CalendarDayOfWeek = DimDates_df.DateID.apply(lambda x: x.weekday())


## Convert back to int
DimDates_df.CalendarQuarter = DimDates_df.CalendarQuarter.astype('int32')
DimDates_df.CalendarMonth = DimDates_df.CalendarMonth.astype('int32')
DimDates_df.CalendarDayOfWeek = DimDates_df.CalendarDayOfWeek.astype('int32')

## Upload Transformed DF onto Database
DimDates_df.to_sql("DimDates", conn, if_exists="replace", index=False)

## Preview tables
FactStore_df = pd.read_sql("SELECT * FROM FactStore", conn)
DimStores_df = pd.read_sql("SELECT * FROM DimStores", conn)
DimDates_df = pd.read_sql("SELECT * FROM DimDates", conn)
DimProducts_df = pd.read_sql("SELECT * FROM DimProducts", conn)


