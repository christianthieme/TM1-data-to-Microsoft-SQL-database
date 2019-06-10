# -*- coding: utf-8 -*-
"""
Created on Tue Apr 30 10:19:53 2019

@author: cthieme
"""

#import necessary modules
from TM1py import TM1Service
import pandas as pd
from TM1py.Utils import Utils
import configparser
from sqlalchemy import create_engine
import sqlalchemy
import urllib

#implement ability to use configuration file
config = configparser.ConfigParser()
config.read(r'C:\JUNK\Python Stuff\config.ini')

with TM1Service(**config['xxxxxx']) as tm2:
    #MDX query to get financial data
    mdx = '''
    SELECT NON EMPTY TM1SubsetToSet([FIN Income Statement Measure],"Default") on COLUMNS,
    {TM1SubsetToSet([FIN Version],"Default")}
    *{[FIN Accounting].[GAAP],[FIN Accounting].[Non-GAAP],[FIN Accounting].[Detail Non-GAAP]}
    *TM1SubsetToSet([FIN Month],"Default") on ROWS
    FROM [FIN Income Statement] 
    '''
    #converting query data to dataframe
    df = tm2.cubes.cells.execute_mdx(mdx = mdx)
    df1 = Utils.build_pandas_dataframe_from_cellset(df, multiindex = False)
    
    #Logging out of TM1 
    tm2.logout()
    
    #filling in null values
df1.dropna(inplace = True)

#Create lists of items not to include from each column    
not_include_from_months = ['..', 'Custom Sum of Qtrs']
not_include_from_measure = ['.', '..','***************', '++++++++++', '+-+-+-+-+-+', '+*+*+*+*+*+', 'Actual/Forecast', 'OVERIDE INPUTS', 'SUPPLEMENTAL DISCLOSURES', 'SUPPLEMENTAL DATA']
not_include_from_version = ['A1_16', 'B1_16', 'C1_16', 'D1_16', 'A1_17', 'A1_17 BOD', 'B1_17', 'C1_17', 'D1_17']

#filtering to show only those items not in the lists above
df1 = df1.loc[~df1.loc[:,'FIN Month'].isin(not_include_from_months)]
df1 = df1.loc[~df1.loc[:,'FIN Income Statement Measure'].isin(not_include_from_measure)]
df1 = df1.loc[~df1.loc[:,'FIN Version'].isin(not_include_from_version)]

#Changing column names to database format
df1.columns = ['fin_version', 'fin_accounting', 'fin_entity', 'fin_month', 'fin_income_statement_measure', 'fin_values']

#creating connection to Microsoft SQL Server    
    
params = urllib.parse.quote_plus('Driver={SQL Server};'
                      'Server=SQL_Server;'
                      'Database=server_data;'
                      'Trusted_Connection=yes;')

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

#Pushing dataframe to SQL Server
df1.to_sql('income_statement', schema='dbo', con = engine, if_exists = 'replace', index = False, 
           dtype={
               'fin_version': sqlalchemy.types.VARCHAR(length=15), 
               'fin_accounting': sqlalchemy.types.VARCHAR(length=15),
               'fin_entity': sqlalchemy.types.VARCHAR(length=20),
               'fin_month': sqlalchemy.types.VARCHAR(length=15),
               'fin_income_statement_measure': sqlalchemy.types.VARCHAR(length=300),
               'fin_values': sqlalchemy.types.Float()
           }
          )

#Closing connection
engine.dispose()
 

    
    
