import psycopg2
import csv
import pandas as pd
import numpy as np

def map_appstatus(s):
    if s == 'PENDING_FOR_PAYMENT':
        return 'Pending for Payment'
    elif s == 'PENDING_FOR_CITIZEN_ACTION':
        return 'Pending for Citizen Action'
    elif s == 'PENDING_FOR_FIELD_INSPECTION':
        return 'Pending for Field Inspection'
    elif s == 'REJECTED':
        return 'Rejected'
    elif s == 'APPROVED':
        return 'Approved'
    elif s == 'PENDING_FOR_APPROVAL':
        return 'Pending for Approval'
    elif s == 'CONNECTION_ACTIVATED':
        return 'Connection Activated'
    elif s == 'PENDING_APPROVAL_FOR_CONNECTION':
        return 'Pending Approval for Connection'
    elif s == 'PENDING_FOR_CONNECTION_ACTIVATION':
        return 'Pending for Connection Activation'
    elif s == 'PENDING_FOR_DOCUMENT_VERIFICATION':
        return 'Pending for Document Verification'
    elif s == 'INITIATED':
        return 'Initiated'
    

def connect():
    try:
        conn = psycopg2.connect(database="{{REPLACE-WITH-DATABASE}}", user="{{REPLACE-WITH-USERNAME}}",
                            password="{{REPLACE-WITH-PASSWORD}}", host="{{REPLACE-WITH-HOST}}")
        print("Connection established!")
        
    except Exception as exception:
        print("Exception occurred while connecting to the database")
        print(exception)
   
   
    waterquery = pd.read_sql_query("SELECT cn.applicationno AS \"Application Number\", cn.id AS \"Connection Id\",cn.connectionno AS \"Connection Number\", cn.status AS \"Status\",   srv.connectiontype AS \"Connection Type\",    cn.applicationstatus AS \"Application Status\",      to_timestamp(CAST(cn.createdtime AS bigint)/1000)::date AS \"Application Start Datetime\", CASE WHEN cn.applicationstatus = 'PENDING_FOR_FIELD_INSPECTION' THEN to_timestamp(CAST(cn.lastmodifiedtime AS bigint)/1000)::date END AS \"Application Verified and Sent Forward Datetime\",CASE WHEN cn.applicationstatus = 'PENDING_FOR_CONNECTION_ACTIVATION' THEN to_timestamp(CAST(cn.lastmodifiedtime AS bigint)/1000)::date END AS \"Application Approved Datetime\", CASE WHEN cn.applicationstatus = 'CONNECTION_ACTIVATED' THEN to_timestamp(CAST(cn.lastmodifiedtime AS bigint)/1000)::date END AS \"Connection Activated Datetime\", to_timestamp (CAST(srv.connectionexecutiondate AS bigint)/1000)::date AS \"Connection Execution Datetime\", pt.district AS \"District\", SUBSTRING(pt.tenantid, 4) AS \"City\", pt.state AS \"State\", CASE WHEN hld.userid = ptown.userid then 'Yes' else 'No' END AS \"Property Owner is Connection Holder\" FROM (eg_ws_connection cn INNER JOIN eg_ws_service srv ON cn.id = srv.connection_id) INNER JOIN eg_pt_address pt ON pt.propertyid = cn.property_id LEFT OUTER JOIN eg_ws_connectionholder hld ON cn.id = hld.connectionid INNER JOIN eg_pt_owner ptown ON ptown.propertyid = cn.property_id INNER JOIN eg_ws_connection_Audit aud ON cn.id = aud.id WHERE cn.status != 'INACTIVE' AND cn.tenantId != 'pb.testing'", conn)
    waterpaymentquery = pd.read_sql_query("SELECT ws.applicationno AS \"Application Number\",ep.totaldue AS \"Total Amount Due\", ep.totalamountpaid AS \"Total Amount Paid\",ep.paymentmode AS \"Payment Mode\", ep.paymentstatus AS \"Payment Status\", ws.adhocpenalty AS \"Penalty\",ws.adhocrebate AS \"Interest\" FROM egcl_payment ep INNER JOIN egcl_paymentdetail epd ON ep.id=epd.paymentid INNER JOIN egcl_bill eb ON eb.id=epd.billid INNER JOIN eg_ws_connection ws ON ws.applicationno=eb.consumercode WHERE ws.status != 'INACTIVE' AND ws.tenantId != 'pb.testing'",conn)
    seweragepaymentquery = pd.read_sql_query("SELECT sw.applicationno AS \"Application Number\", ep.totaldue AS \"Total Amount Due\", ep.totalamountpaid AS \"Total Amount Paid\", ep.paymentmode AS \"Payment Mode\", ep.paymentstatus AS \"Payment Status\", sw.adhocpenalty AS \"Penalty\", sw.adhocrebate AS \"Interest\" FROM egcl_payment ep INNER JOIN egcl_paymentdetail epd ON ep.id=epd.paymentid INNER JOIN egcl_bill eb ON eb.id=epd.billid INNER JOIN eg_sw_connection sw ON sw.applicationno=eb.consumercode WHERE sw.status != 'INACTIVE' AND sw.tenantId != 'pb.testing'",conn)
    seweragequery = pd.read_sql_query("SELECT cn.applicationno AS \"Application Number\", cn.id AS \"Connection Id\",cn.connectionno AS \"Connection Number\", cn.status AS \"Status\", cn.applicationstatus AS \"Application Status\",      to_timestamp(CAST(cn.createdtime AS bigint)/1000)::date AS \"Application Start Datetime\", CASE WHEN cn.applicationstatus = 'PENDING_FOR_FIELD_INSPECTION' THEN to_timestamp(CAST(cn.lastmodifiedtime AS bigint)/1000)::date END AS \"Application Verified and Sent Forward Datetime\",CASE WHEN cn.applicationstatus = 'PENDING_FOR_CONNECTION_ACTIVATION' THEN to_timestamp(CAST(cn.lastmodifiedtime AS bigint)/1000)::date END AS \"Application Approved Datetime\", CASE WHEN cn.applicationstatus = 'CONNECTION_ACTIVATED' THEN to_timestamp(CAST(cn.lastmodifiedtime AS bigint)/1000)::date END AS \"Connection Activated Datetime\", to_timestamp (CAST(srv.connectionexecutiondate AS bigint)/1000)::date AS \"Connection Execution Datetime\", pt.district AS \"District\", SUBSTRING(pt.tenantid, 4) AS \"City\", pt.state AS \"State\", CASE WHEN hld.userid = ptown.userid then 'Yes' else 'No' END AS \"Property Owner is Connection Holder\" FROM (eg_sw_connection cn  INNER JOIN eg_sw_service srv ON cn.id = srv.connection_id) INNER JOIN eg_pt_address pt ON pt.propertyid = cn.property_id LEFT OUTER JOIN eg_sw_connectionholder hld ON cn.id = hld.connectionid INNER JOIN eg_pt_owner ptown ON ptown.propertyid = cn.property_id WHERE cn.status != 'INACTIVE' AND cn.tenantId != 'pb.testing'",conn)
  
    watergen = pd.DataFrame(waterquery)
    waterpayment = pd.DataFrame(waterpaymentquery)
    seweragegen = pd.DataFrame(seweragequery)
    seweragepayment = pd.DataFrame(seweragepaymentquery)
    
    waterdata = pd.DataFrame()
    seweragedata = pd.DataFrame()

    waterdata = pd.merge(watergen,waterpayment,left_on='Application Number',right_on='Application Number',how='left')
    seweragedata = pd.merge(seweragegen, seweragepayment,left_on='Application Number',right_on='Application Number',how='left' )
   
    waterdata['Application Status'] = waterdata['Application Status'].map(map_appstatus)    
    seweragedata['Application Status'] = seweragedata['Application Status'].map(map_appstatus)    
   
    waterdata["City"]= waterdata["City"].str.upper().str.title()
    seweragedata["City"]= seweragedata["City"].str.upper().str.title()

    waterdata = waterdata.rename(columns={"Application Start Datetime": "Application_Start_Datetime","Application Verified and Sent Forward Datetime":"Application_Verified_and_Sent_Forward_Datetime","Application Approved Datetime":"Application_Approved_Datetime","Connection Activated Datetime":"Connection_Activated_Datetime","Connection Execution Datetime":"Connection_Execution_Datetime"})
    seweragedata = seweragedata.rename(columns={"Application Start Datetime": "Application_Start_Datetime","Application Verified and Sent Forward Datetime":"Application_Verified_and_Sent_Forward_Datetime","Application Approved Datetime":"Application_Approved_Datetime","Connection Activated Datetime":"Connection_Activated_Datetime","Connection Execution Datetime":"Connection_Execution_Datetime"})
  
    waterdata['Application_Start_Datetime'] = pd.to_datetime(waterdata.Application_Start_Datetime, format='%Y-%m-%d')
    waterdata['Application_Verified_and_Sent_Forward_Datetime'] = pd.to_datetime(waterdata.Application_Verified_and_Sent_Forward_Datetime, format='%Y-%m-%d')
    waterdata['Application_Approved_Datetime'] = pd.to_datetime(waterdata.Application_Approved_Datetime, format='%Y-%m-%d')
    waterdata['Connection_Activated_Datetime'] = pd.to_datetime(waterdata.Connection_Activated_Datetime, format='%Y-%m-%d')
    waterdata['Connection_Execution_Datetime'] = pd.to_datetime(waterdata.Connection_Execution_Datetime, format='%Y-%m-%d')

    seweragedata['Application_Start_Datetime'] = pd.to_datetime(seweragedata.Application_Start_Datetime, format='%Y-%m-%d')
    seweragedata['Application_Verified_and_Sent_Forward_Datetime'] = pd.to_datetime(seweragedata.Application_Verified_and_Sent_Forward_Datetime, format='%Y-%m-%d')
    seweragedata['Application_Approved_Datetime'] = pd.to_datetime(seweragedata.Application_Approved_Datetime, format='%Y-%m-%d')
    seweragedata['Connection_Activated_Datetime'] = pd.to_datetime(seweragedata.Connection_Activated_Datetime, format='%Y-%m-%d')
    seweragedata['Connection_Execution_Datetime'] = pd.to_datetime(seweragedata.Connection_Execution_Datetime, format='%Y-%m-%d')

    waterdata['Application_Start_Datetime'] = waterdata['Application_Start_Datetime'].dt.strftime("%d-%m-%y")
    waterdata['Application_Verified_and_Sent_Forward_Datetime'] = waterdata['Application_Verified_and_Sent_Forward_Datetime'].dt.strftime("%d-%m-%y")
    waterdata['Application_Approved_Datetime'] = waterdata['Application_Approved_Datetime'].dt.strftime("%d-%m-%y")
    waterdata['Connection_Activated_Datetime'] = waterdata['Connection_Activated_Datetime'].dt.strftime("%d-%m-%y")
    waterdata['Connection_Execution_Datetime'] = waterdata['Connection_Execution_Datetime'].dt.strftime("%d-%m-%y")

    seweragedata['Application_Start_Datetime'] = seweragedata['Application_Start_Datetime'].dt.strftime("%d-%m-%y")
    seweragedata['Application_Verified_and_Sent_Forward_Datetime'] = seweragedata['Application_Verified_and_Sent_Forward_Datetime'].dt.strftime("%d-%m-%y")
    seweragedata['Application_Approved_Datetime'] = seweragedata['Application_Approved_Datetime'].dt.strftime("%d-%m-%y")
    seweragedata['Connection_Activated_Datetime'] = seweragedata['Connection_Activated_Datetime'].dt.strftime("%d-%m-%y")
    seweragedata['Connection_Execution_Datetime'] = seweragedata['Connection_Execution_Datetime'].dt.strftime("%d-%m-%y") 
    
    waterdata = waterdata.rename(columns={"Application_Start_Datetime":"Application Start Datetime","Application_Verified_and_Sent_Forward_Datetime":"Application Verified and Sent Forward Datetime","Application_Approved_Datetime":"Application Approved Datetime","Connection_Activated_Datetime":"Connection Activated Datetime","Connection_Execution_Datetime":"Connection Execution Datetime"})
    seweragedata = waterdata.rename(columns={"Application_Start_Datetime":"Application Start Datetime","Application_Verified_and_Sent_Forward_Datetime":"Application Verified and Sent Forward Datetime","Application_Approved_Datetime":"Application Approved Datetime","Connection_Activated_Datetime":"Connection Activated Datetime","Connection_Execution_Datetime":"Connection Execution Datetime"})
    
    waterdata.fillna('', inplace=True)
    seweragedata.fillna('', inplace=True)
    
    waterdata.to_csv('/tmp/waterDatamart.csv')
    seweragedata.to_csv('/tmp/sewerageDatamart.csv')

    print("Datamart exported. Please copy it using kubectl cp command to you required location.")
    
if __name__ == '__main__':
    connect()

