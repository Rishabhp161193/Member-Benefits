import pyodbc
import pandas
import datetime
from datetime import datetime,timedelta

#Establishing RSTS connection and defining variables
conn = pyodbc.connect('DSN=NDC')
today=datetime.now()
monday=today-timedelta(days=today.weekday())
monday=str(monday.date())

#Extracting KPIs
querypl = """SELECT
  Table__1."COUNTRY_CODE",
  Table__1."CALMONTH",
  Table__1."CALWEEK",
  Table__1."START_DATE",
  Table__1."LOYALTY_IND",
  Table__1."IPH_SUB_CATEGORY_NAME" as "CATEGORY",
  SUM( Table__1."CM_NUMBER_OF_TRANS_001"),
  SUM(Table__1."SALES_QUANTITY"),
  SUM(Table__1."CM_NET_SALES_001")
FROM
  "_SYS_BIC"."shell.app.retail/CA_GENERAL_SALES_GRA_001"  Table__1
WHERE
   Table__1."COUNTRY_CODE"  IN  ( 'PL'  )
   AND
   Table__1."CALENDAR_YEAR_C" in (2021,2022) 
   AND
   Table__1."IPH_SUB_CATEGORY_NAME"  IN  ( 'Hot Dogs/Burgers','Water','Coffee Own Brands','Roll Over','MOGAS Maingrade','On-Road Automotive Gas Oil','Auto Gas','Screen Wash'  )
   
GROUP BY
  Table__1."COUNTRY_CODE", 
  Table__1."CALMONTH", 
  Table__1."CALWEEK",
  Table__1."START_DATE",
  Table__1."LOYALTY_IND", 
  Table__1."IPH_SUB_CATEGORY_NAME"
"""
Memberbenefitspl = pandas.read_sql(querypl,conn)

querybg = """SELECT
  Table__1."COUNTRY_CODE",
  Table__1."CALMONTH",
  Table__1."CALWEEK",
  Table__1."START_DATE",
  Table__1."LOYALTY_IND",
  Table__1."IPH_SUB_CATEGORY_NAME" as "CATEGORY",
  SUM( Table__1."CM_NUMBER_OF_TRANS_001"),
  SUM(Table__1."SALES_QUANTITY"),
  SUM(Table__1."CM_NET_SALES_001")
FROM
  "_SYS_BIC"."shell.app.retail/CA_GENERAL_SALES_GRA_001"  Table__1
WHERE
  (
   Table__1."COUNTRY_CODE"  IN  ( 'BG'  )
   AND
   Table__1."CALENDAR_YEAR_C" in (2021,2022) AND
(
     Table__1."IPH_SUB_CATEGORY_NAME"  IN  ( 'Water','Energy Drinks','Hot Dogs/Burgers','Screen Wash')))
GROUP BY
  Table__1."COUNTRY_CODE", 
  Table__1."CALMONTH", 
  Table__1."CALWEEK",
  Table__1."START_DATE",
  Table__1."LOYALTY_IND", 
  Table__1."IPH_SUB_CATEGORY_NAME"
"""
Memberbenefitsbg = pandas.read_sql(querybg,conn)
Memberbenefitstotal=Memberbenefitspl.append(Memberbenefitsbg,sort=True)
Memberbenefitstotal.to_csv('C:\\Users\\Rishabh.pandey\\OneDrive - Shell\\CEE data\\CRM-Member benefits\\Member Benefits test.csv')
Memberbenefitsmapping=pandas.read_csv('C:\\Users\\Rishabh.pandey\\OneDrive - Shell\\CEE data\\CRM-Member benefits\\Member Benefit Mapping.csv')

queryhu = """SELECT
  Table__1."COUNTRY_CODE",
  Table__1."CALMONTH",
  Table__1."CALWEEK",
  Table__1."START_DATE",
  Table__1."LOYALTY_IND",
  Table__1."IPH_SUB_CATEGORY_NAME" as "CATEGORY",
  SUM( Table__1."CM_NUMBER_OF_TRANS_001"),
  SUM(Table__1."SALES_QUANTITY"),
  SUM(Table__1."CM_NET_SALES_001")
FROM
  "_SYS_BIC"."shell.app.retail/CA_GENERAL_SALES_GRA_001"  Table__1
WHERE
   Table__1."COUNTRY_CODE"  IN  ('HU')
   AND
   Table__1."CALENDAR_YEAR_C" in (2021,2022)
   AND
   Table__1."IPH_SUB_CATEGORY_NAME"  IN  ('Energy Drinks','Water','PCMO Engine Oils PCMO')
GROUP BY
  Table__1."COUNTRY_CODE", 
  Table__1."CALMONTH", 
  Table__1."CALWEEK",
  Table__1."START_DATE",
  Table__1."LOYALTY_IND", 
  Table__1."IPH_SUB_CATEGORY_NAME"
"""
Memberbenefitshu = pandas.read_sql(queryhu,conn)
Memberbenefitstotalhu=Memberbenefitstotal.append(Memberbenefitshu,sort=True)
#Combos specific queries
queryPLkokasanka="""SELECT
  Table__1."COUNTRY_CODE",
  Table__1."LOYALTY_IND",
  Table__1."CALMONTH",
  Table__1."CALWEEK",
  Table__1."START_DATE",
  'COFEE AND kokasanka' as "CATEGORY",
  SUM( Table__1."CM_NUMBER_OF_TRANS_001"),
  SUM(Table__1."SALES_QUANTITY"),
  SUM(Table__1."CM_NET_SALES_001")
FROM
  "_SYS_BIC"."shell.app.retail/CA_GENERAL_SALES_GRA_001"  Table__1
WHERE
  (
   Table__1."IPH_SUB_CATEGORY_NAME"  IN  ( 'Coffee Own Brands' )
   AND
   Table__1."COUNTRY_CODE"  IN  ( 'PL'  )
   AND
   Table__1."CALENDAR_YEAR_C" in (2021,2022) AND
   Table__1."RETAIL_TRANSACTION_ID"  IN  
     (
     SELECT
       Table__1."RETAIL_TRANSACTION_ID"
     FROM
       "_SYS_BIC"."shell.app.retail/CA_GENERAL_SALES_GRA_001"  Table__1
     WHERE
       (
        Table__1."IPH_SUB_CATEGORY_NAME"  IN  ( 'Baked Confectionary'  )
        AND
        Table__1."COUNTRY_CODE"  IN  ( 'PL'  )
        AND
         Table__1."CALENDAR_YEAR_C" in (2021,2022) 
          )
     )
  )
GROUP BY
  Table__1."COUNTRY_CODE", 
  Table__1."LOYALTY_IND", 
  Table__1."CALMONTH", 
  Table__1."CALWEEK",
  Table__1."START_DATE",
  Table__1."IPH_CATEGORY_NAME", 
  Table__1."IPH_SUB_CATEGORY_NAME"
  """
Memberbenefitskoksas = pandas.read_sql(queryPLkokasanka,conn)
Memberbenefitstotal1=Memberbenefitstotalhu.append(Memberbenefitskoksas,sort=True)

queryPLkajzerka="""SELECT
  Table__1."COUNTRY_CODE",
  Table__1."LOYALTY_IND",
  Table__1."CALMONTH",
  Table__1."CALWEEK",
  Table__1."START_DATE",
  'COFEE/JUICE AND kajzerka' as "CATEGORY",
  SUM( Table__1."CM_NUMBER_OF_TRANS_001"),
  SUM(Table__1."SALES_QUANTITY"),
  SUM(Table__1."CM_NET_SALES_001")
FROM
  "_SYS_BIC"."shell.app.retail/CA_GENERAL_SALES_GRA_001"  Table__1
WHERE
  (
   Table__1."IPH_SUB_CATEGORY_NAME"  IN  ( 'Coffee Own Brands','Juices'  )
   AND
   Table__1."COUNTRY_CODE"  IN  ( 'PL'  )
   AND
   Table__1."CALENDAR_YEAR_C" in (2021,2022) AND
   Table__1."RETAIL_TRANSACTION_ID"  IN  
     (
     SELECT
       Table__1."RETAIL_TRANSACTION_ID"
     FROM
       "_SYS_BIC"."shell.app.retail/CA_GENERAL_SALES_GRA_001"  Table__1
     WHERE
       (
        Table__1."IPH_SUB_CATEGORY_NAME"  IN  ( 'Sandwiches/Wraps/Rolls'  )
        AND
        Table__1."COUNTRY_CODE"  IN  ( 'PL'  )
        AND
         Table__1."CALENDAR_YEAR_C" in (2021,2022) )
     )
  )
GROUP BY
  Table__1."COUNTRY_CODE", 
  Table__1."LOYALTY_IND", 
  Table__1."CALMONTH", 
  Table__1."CALWEEK",
  Table__1."START_DATE",
  Table__1."IPH_CATEGORY_NAME", 
  Table__1."IPH_SUB_CATEGORY_NAME"
  """
Memberbenefitkajszerka = pandas.read_sql(queryPLkajzerka,conn)
Memberbenefitstotal2=Memberbenefitstotal1.append(Memberbenefitkajszerka,sort=True)

queryPLpanini="""SELECT
  Table__1."COUNTRY_CODE",
  Table__1."LOYALTY_IND",
  Table__1."CALMONTH",
  Table__1."CALWEEK",
  Table__1."START_DATE",
  'COFEE/JUICE AND panini' as "CATEGORY",
  SUM( Table__1."CM_NUMBER_OF_TRANS_001"),
  SUM(Table__1."SALES_QUANTITY"),
  SUM(Table__1."CM_NET_SALES_001")
FROM
  "_SYS_BIC"."shell.app.retail/CA_GENERAL_SALES_GRA_001"  Table__1
WHERE
  (
   Table__1."IPH_SUB_CATEGORY_NAME"  IN  ( 'Coffee Own Brands','Juices'  )
   AND
   Table__1."COUNTRY_CODE"  IN  ( 'PL'  )
   AND
   Table__1."CALENDAR_YEAR_C" in (2021,2022) AND
   Table__1."RETAIL_TRANSACTION_ID"  IN  
     (
     SELECT
       Table__1."RETAIL_TRANSACTION_ID"
     FROM
       "_SYS_BIC"."shell.app.retail/CA_GENERAL_SALES_GRA_001"  Table__1
     WHERE
       (
        Table__1."IPH_SUB_CATEGORY_NAME"  IN  ( 'Sandwiches/Wraps/Rolls'  )
        AND
        Table__1."COUNTRY_CODE"  IN  ( 'PL'  )
        AND
        Table__1."CALENDAR_YEAR_C" in (2021,2022) )
     )
  )
GROUP BY
  Table__1."COUNTRY_CODE", 
  Table__1."LOYALTY_IND", 
  Table__1."CALMONTH", 
  Table__1."CALWEEK",
  Table__1."START_DATE",
  Table__1."IPH_CATEGORY_NAME", 
  Table__1."IPH_SUB_CATEGORY_NAME"
  """
Memberbenefitpanini = pandas.read_sql(queryPLpanini,conn)
Memberbenefitstotal3=Memberbenefitstotal2.append(Memberbenefitpanini,sort=True)
Memberbenefitstotal3=Memberbenefitstotal3[pandas.to_datetime(Memberbenefitstotal3['START_DATE'])<pandas.to_datetime(monday)]

#Creating TY and LY structure
MemberBenefitdataTY=Memberbenefitstotal3
MemberBenefitdataLY=MemberBenefitdataTY
MemberBenefitdataTY['START_DATE']=pandas.to_datetime(MemberBenefitdataTY['START_DATE'])
MemberBenefitdataTY['PREV_YEAR_START_DATE']=MemberBenefitdataTY['START_DATE']-timedelta(days=365)
MemberBenefitdatafinal=pandas.merge(MemberBenefitdataTY,MemberBenefitdataLY,left_on=["PREV_YEAR_START_DATE","CATEGORY","COUNTRY_CODE","LOYALTY_IND"],right_on=["START_DATE","CATEGORY","COUNTRY_CODE","LOYALTY_IND"],how="left")
MemberBenefitdatafinal=MemberBenefitdatafinal.groupby(["CATEGORY","COUNTRY_CODE","LOYALTY_IND","CALMONTH_x","CALWEEK_x"],as_index=False).sum()
MemberBenefitdatafinal=pandas.merge(MemberBenefitdatafinal,Memberbenefitsmapping,how='left',left_on=['COUNTRY_CODE','CATEGORY'],right_on=['COUNTRY','Category'])
MemberBenefitdatafinal.to_csv('C:\\Users\\Rishabh.pandey\\OneDrive - Shell\\CEE data\\CRM-Member benefits\\Member Benefits RSTS.csv')

