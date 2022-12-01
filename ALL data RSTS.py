import pyodbc
import pandas
from datetime import datetime,timedelta
from dateutil import relativedelta
#Establishing RSTS connection
conn = pyodbc.connect('DSN=NDC')
today=datetime.now()
monday=today-timedelta(days=today.weekday())
monday=str(monday.date())

#Getting total data
query="""SELECT
  Table__1."COUNTRY_CODE",
  Table__1."CLASS_NAME",
  Table__1."CALMONTH",
  Table__1."CALWEEK",
  Table__1."START_DATE",
  Table__1."LOYALTY_IND",
  SUM(Table__1."SALES_QUANTITY"),
  SUM( Table__1."CM_NUMBER_OF_TRANS_001"),
  SUM(Table__1."CM_NET_SALES_001")
FROM
  "_SYS_BIC"."shell.app.retail/CA_GENERAL_SALES_GRA_001"  Table__1
WHERE
  (
   Table__1."COUNTRY_CODE"  IN  ( 'BG','PL','HU')
   AND
   Table__1."CALENDAR_YEAR_C" in (2021,2022))
GROUP BY
  Table__1."COUNTRY_CODE", 
  Table__1."CLASS_NAME", 
  Table__1."CALMONTH", 
  Table__1."CALWEEK",
  Table__1."START_DATE",
  Table__1."LOYALTY_IND"
"""
Alldata = pandas.read_sql(query,conn)
Alldata=Alldata[pandas.to_datetime(Alldata['START_DATE'])<pandas.to_datetime(monday)]
AlldataTY=Alldata
AlldataLY=Alldata
AlldataTY['START_DATE']=pandas.to_datetime(AlldataTY['START_DATE'])
AlldataTY['PREV_YEAR_START_DATE']=AlldataTY['START_DATE']-timedelta(days=365)
Alldatafinal=pandas.merge(AlldataTY,AlldataLY,left_on=["PREV_YEAR_START_DATE","CLASS_NAME","COUNTRY_CODE","LOYALTY_IND"],right_on=["START_DATE","CLASS_NAME","COUNTRY_CODE","LOYALTY_IND"],how="left")
Alldatafinal=Alldatafinal.groupby(["CLASS_NAME","COUNTRY_CODE","LOYALTY_IND","CALMONTH_x","CALWEEK_x"],as_index=False).sum()
Alldatafinal.to_csv('C:\\Users\\Rishabh.pandey\\OneDrive - Shell\\CEE data\\CRM-Member benefits\\All data.csv')
