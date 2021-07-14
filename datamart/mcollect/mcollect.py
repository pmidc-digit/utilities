    
import psycopg2
import csv
import pandas as pd
import numpy as np

def map_bs(s):
    if s == 'TX.TS1_copy_register_for_old_survey':
        return 'Taxes - TS1 copy register for old survey'
    elif s == 'ADVT.Unipolls':
        return 'Advertisement Tax - Unipolls'
    elif s == 'ADVT.Hoardings':
        return 'Advertisement Tax - Hoardings'
    elif s == 'ADVT.Gas_Balloon_Advertisement':
        return 'Advertisement Tax - Gas Balloon Advertisement'
    elif s == 'TX.Transfer_Property_Fees':
        return 'Taxes - Transfer Property Fees'
    elif s == 'RT.Municipal_Shops_Rent':
        return 'Rents - Municipal Shops Rent'
    elif s == 'ADVT.Wall_Paint_Advertisement':
        return 'Advertisement Tax - Wall Paint Advertisement'
    elif s == 'TX.No_Dues_Certificate':
        return 'Taxes - No Dues Certificate'
    elif s == 'ADVT.Light_Wala_Board':
        return 'Advertisement Tax - Light Wala Board'
  

def connect():
    try:
        conn = psycopg2.connect(database="{{REPLACE-WITH-DATABASE}}", user="{{REPLACE-WITH-USERNAME}}",
                            password="{{REPLACE-WITH-PASSWORD}}", host="{{REPLACE-WITH-HOST}}")  
        print("Connection established!")
   
    except Exception as exception:
        print("Exception occurred while connecting to the database")
        print(exception)

   
    mCollectquery = pd.read_sql_query("SELECT chl.challanNo AS \"Challan Number\", chl.businessService AS \"Business Service\", INITCAP(chl.applicationstatus) AS \"Application Status\", adr.locality AS \"Locality\", INITCAP( SUBSTRING(adr.tenantId, 4)) AS \"City\", adr.state AS \"State\", ep.totaldue As \"Total Amount Due\", ep.totalamountpaid as \"Total Amount Paid\", ep.paymentmode AS \"Payment Mode\",ep.paymentstatus AS \"Payment Status\",eb.billnumber AS \"Bill Number\", eb.status as \"Bill Status\" FROM eg_echallan chl INNER JOIN eg_challan_address adr ON chl.id=adr.echallanid LEFT OUTER JOIN egcl_bill eb ON chl.challanno=eb.consumercode LEFT OUTER JOIN egcl_paymentdetail epd ON eb.id=epd.billid LEFT OUTER JOIN egcl_payment ep ON ep.id=epd.paymentid", conn)
    print("Connection established!")
    mcollectgen = pd.DataFrame(mCollectquery)
    mcollectgen['Business Service'] = mcollectgen['Business Service'].map(map_bs)
    mcollectgen.to_csv('/tmp/mcollectDatamart.csv')
    print("Datamart exported. Please copy it using kubectl cp command to you required location.")
    
if __name__ == '__main__':
    connect()

