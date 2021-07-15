import psycopg2
import csv
import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime

def map_MC(s):
    if s in MC:
        return 'MC'
    elif s in MC1:
        return 'MC1'
    elif s in MC2:
        return 'MC2'
    elif s in MC3:
        return 'MC3'
    elif s in NP:
        return 'NP'

MC=['pb.abohar',
               'pb.amritsar',
               'pb.batala',
               'pb.bathinda',
               'pb.hoshiarpur',
               'pb.jalandhar',
               'pb.kapurthala',
               'pb.ludhiana',
               'pb.moga',
               'pb.mohali',
               'pb.pathankot',
               'pb.patiala',
               'pb.phagwara']

MC1 = ['pb.anandpursahib',
               'pb.barnala',
               'pb.derabassi',
               'pb.faridkot',
               'pb.fazilka',
               'pb.ferozepur',
               'pb.gurdaspur',
               'pb.jagraon',
               'pb.khanna',
               'pb.kharar',
               'pb.kotkapura',
               'pb.lalru',
               'pb.malerkotla',
               'pb.malout',
               'pb.mandigobindgarh',
               'pb.mansa',
               'pb.muktsar',
               'pb.nabha',
               'pb.nakodar',
               'pb.nangal',
               'pb.nawanshahr',
               'pb.rajpura',
               'pb.samana',
               'pb.sangrur',
               'pb.sunam',
               'pb.zirakpur']

MC2 = ['pb.adampur',
               	'pb.ahmedgarh',
               	'pb.baghapurana',
               	'pb.banga',
               	'pb.bhawanigarh',
               	'pb.bhogpur',
               	'pb.bhuchomandi',
               	'pb.budhlada',
               	'pb.dasuya',
               	'pb.dharamkot',
               	'pb.dhariwal',
               	'pb.dhuri',
               	'pb.dinanagar',
               	'pb.doraha',
               	'pb.garhshankar',
               	'pb.gidderbaha',
               	'pb.goniana',
               	'pb.goraya',
               	'pb.guruharsahai',
               	'pb.jaitu',
               	'pb.jalalabad',
               	'pb.jandialaguru',
               	'pb.kartarpur',
               	'pb.kurali',
               	'pb.lehragaga',
               	'pb.maur',
               	'pb.morinda',
               	'pb.mukerian',
               	'pb.mullanpur',
               	'pb.nayagaon',
               	'pb.nurmahal',
               	'pb.patran',
               	'pb.patti',
               	'pb.phillaur',
               	'pb.raikot',
               	'pb.raman',
               	'pb.rampuraphul',
               	'pb.ropar',
               	'pb.sahnewal',
               	'pb.samrala',
               	'pb.sirhind',
               	'pb.sujanpur',
               	'pb.sultanpurlodhi',
               	'pb.talwandibhai',
               	'pb.tarntaran',
               	'pb.urmartanda',
               	'pb.zira']

MC3 = ['pb.alawalpur',
               	'pb.amloh',
               	'pb.balachaur',
               	'pb.banur',
               	'pb.bareta',
               	'pb.bassipathana',
               	'pb.bhadaur',
               	'pb.derababananak',
               	'pb.dhanaula',
               	'pb.fatehgarhchurian',
               	'pb.garhdiwala',
               	'pb.hariana',
               	'pb.kotfatta',
               	'pb.longowal',
               	'pb.machhiwara',
               	'pb.majitha',
               	'pb.payal',
               	'pb.quadian',
               	'pb.rahon',
               	'pb.ramdass',
               	'pb.sanaur',
               	'pb.sangatmandi',
               	'pb.shamchurasi',
               	'pb.srihargobindpur',
               	'pb.tapa']

NP = ['pb.ajnala',
               	'pb.amargarh',
               	'pb.arniwala',
               	'pb.badhnikalan',
               	'pb.balianwali',
               	'pb.bariwala',
               	'pb.begowal',
               	'pb.bhadson',
               	'pb.bhagtabhai',
               	'pb.bhairoopa',
               	'pb.bhikhi',
               	'pb.bhikhiwind',
               	'pb.bhulath',
               	'pb.bilga',
               	'pb.boha',
               	'pb.chamkaursahib',
               	'pb.chaunke',
               	'pb.cheema',
               	'pb.dhilwan',
               	'pb.dirba',
               	'pb.fatehgarhpanjtoor',
               	'pb.ghagga',
               	'pb.ghanaur',
               	'pb.handiaya',
               	'pb.joga',
               	'pb.khamano',
               	'pb.khanauri',
               	'pb.khemkaran',
               	'pb.kiratpur',
               	'pb.kothaguru',
               	'pb.kotissekhan',
               	'pb.kotshamir',
               	'pb.lehramohabbat',
               	'pb.lohiankhas',
               	'pb.mahilpur',
               	'pb.makhu',
               	'pb.mallanwala',
               	'pb.maloud',
               	'pb.maluka',
               	'pb.mamdot',
               	'pb.mandikalan',
               	'pb.mehatpur',
               	'pb.mehraj',
               	'pb.moonak',
               	'pb.mudki',
               	'pb.nadala',
               	'pb.narotjaimalsingh',
               	'pb.nathana',
               	'pb.nihalsinghwala',
               	'pb.rajasansi',
               	'pb.rampura',
               	'pb.rayya',
               	'pb.sardulgarh',
               	'pb.shahkot',
               	'pb.talwandisabo',
               	'pb.talwara']

def map_ownershipsubtype(s):
    if s == 'Institutional':
        return 'Institutional'
    elif s == 'Individual':
        return 'Individual'
    elif s == 'Individual.Singleowner':
        return 'Single Owner'
    elif s == 'Institutionalgovernment.Stategovernment':
        return 'State Government'
    elif s == 'Institutionalgovernment.Ulbgovernment':
        return 'Ulb Government'
    elif s == 'Institutionalgovernment.Centralgovernment':
        return 'Central Government'
    elif s == 'Institutionalprivate.Othersgovernmentinstituition':
        return 'Other Government Instituition'
    elif s == 'Individual.Multipleowners':
        return 'Multiple Owners'
    elif s == 'Institutionalprivate.Privatetrust':
        return 'Private Trust'
    elif s == 'Institutionalprivate.Privatecompany':
        return 'Private Company'
    elif s == 'Institutionalprivate.Othersprivateinstituition':
        return 'Other Private Instituition'
    elif s == 'Institutionalprivate.Ngo':
        return 'Ngo'
    elif s == 'Institutionalprivate.Privateboard':
        return 'Private Board'
    
def map_ownershiptype(s):
    if s in Institutional:
        return 'Institutional'
    elif s in InstitutionalPrivate:
        return 'Institutional Private'
    elif s in InstitutionalGovernment:
        return 'Institutional Government'
    elif s in Individual:
        return 'Individual'

Institutional = ['Institutional']

InstitutionalPrivate = ['Other Private Instituition','Private Company','Private Board', 'Private Trust','Ngo']

InstitutionalGovernment = ['Other Government Instituition','State Government','Central Government','Ulb Government',]

Individual = ['Individual','Multiple Owners','Single Owner']

def map_status(s):
    if s == 'Pendingpayment':
        return 'Pending Payment'
    elif s == 'Pendingapproval':
        return 'Pending Approval' 
    elif s == 'Fieldinspection':
        return 'Field Inspection'
    elif s == 'Citizenactionrequired':
        return 'Citizen Action Required'
    elif s == 'Approved':
        return 'Approved'
    elif s == 'Expired':
        return 'Expired'
    elif s == 'Initiated':
        return 'Initiated'
    elif s == 'Cancelled':
        return 'Cancelled'
    elif s == 'Applied':
        return 'Applied'
    elif s == 'Rejected':
        return 'Rejected'
    elif s == 'Paid':
        return 'Paid'


def map_structuretype(s):
    if s == 'Immovable.Kutcha':
        return 'Immovable Kutcha'
    elif s == 'Immovable.Pucca':
        return 'Immovable Pucca'
    
def connect():
    try:
        conn = psycopg2.connect(database="{{REPLACE-WITH-DATABASE}}", user="{{REPLACE-WITH-USERNAME}}",
                            password="{{REPLACE-WITH-PASSWORD}}", host="{{REPLACE-WITH-HOST}}")
    except Exception as exception:
        print("Exception occurred while connecting to the database")
        print(exception)
    
        
    newdataquery = pd.read_sql_query("SELECT tl.applicationNumber AS \"Application Number\",   to_timestamp(CAST(tl.applicationDate AS bigint)/1000)::date AS \"Application Date\",tl.tenantid, tl.validto AS \"Valid Till\", tl.financialYear AS \"Financial Year\", to_timestamp(CAST(tl.commencementDate AS bigint)/1000)::date  AS \"Commencement Date\", tl.licenseNumber as \"License Number\", INITCAP(tl.status) AS \"Application Status\", INITCAP(tu.tradetype) AS tradetype, INITCAP(tu.tradetype) AS \"Trade Subtype\", tu.uom AS \"Trade UOM\", tu.uomvalue AS \"Trade UOM Value\", INITCAP(acc.uom) AS \"Accessory UOM\", acc.uomvalue AS \"Accessory UOM Value\",acc.accessoryCategory AS \"Accessory Category\" , INITCAP(tld.subOwnerShipCategory) AS \"Ownership Subtype\", INITCAP(tld.structuretype) AS \"Structure Type\", ep.totaldue As \"Total Amount Due\", ep.totalamountpaid as \"Total Amount Paid\", INITCAP(ep.paymentmode) AS \"Payment Mode\", tld.adhocpenalty AS \"Penalty\", to_timestamp(CAST(ep.createdtime AS bigint)/1000)::date AS \"Payment Date\", INITCAP(tl.applicationtype) AS \"Application Type\", INITCAP(SUBSTRING(adr.tenantid,4)) AS \"ULB\", adr.locality AS \"Locality\"  FROM eg_tl_TradeLicense tl INNER JOIN eg_tl_TradeLicenseDetail tld ON tl.id = tld.tradelicenseId INNER JOIN eg_tl_Accessory acc ON tld.id = acc.tradeLicenseDetailId INNER JOIN eg_tl_TradeUnit tu ON tld.id = tu.tradeLicenseDetailId INNER JOIN eg_tl_address adr ON tld.id = adr.tradeLicenseDetailId LEFT OUTER JOIN egcl_bill eb ON  tl.applicationNumber=eb.consumercode LEFT OUTER JOIN egcl_paymentdetail epd ON eb.id=epd.billid LEFT OUTER JOIN  egcl_payment ep ON ep.id=epd.paymentid WHERE tl.applicationtype = 'NEW' AND tl.tenantId != 'pb.testing'", conn)
    renewdataquery = pd.read_sql_query("SELECT tl.tenantid, tl.financialYear AS \"Financial Year\", to_timestamp(CAST(tl.applicationDate AS bigint)/1000)::date AS \"Application Date\", tl.applicationNumber AS \"Application Number\", to_timestamp(CAST(tl.commencementDate AS bigint)/1000)::date  AS \"Commencement Date\", tl.licenseNumber as \"License Number\",tl.oldlicensenumber AS \"Old License Number\", INITCAP(tl.status) AS \"Application Status\", INITCAP(tu.tradetype) AS tradetype, INITCAP(tu.tradetype) AS \"Trade Subtype\", tu.uom AS \"Trade UOM\", tu.uomvalue AS \"Trade UOM Value\", INITCAP(acc.uom) AS \"Accessory UOM\", acc.uomvalue AS \"Accessory UOM Value\", INITCAP(tld.subOwnerShipCategory) AS \"Ownership Subtype\", INITCAP(tld.structuretype) AS \"Structure Type\", tld.adhocpenalty AS \"Penalty\", ep.totaldue As \"Total Amount Due\", ep.totalamountpaid as \"Total Amount Paid\", INITCAP(ep.paymentmode) AS \"Payment Mode\", to_timestamp(CAST(ep.createdtime AS bigint)/1000)::date AS \"Payment Date\", INITCAP(tl.applicationtype) AS \"Application Type\", INITCAP(tl.workflowcode) AS \"Renewal Type\", INITCAP(SUBSTRING(adr.tenantid,4)) AS \"ULB\", adr.locality AS \"Locality\" FROM eg_tl_TradeLicense tl INNER JOIN eg_tl_TradeLicenseDetail tld ON tl.id = tld.tradelicenseId INNER JOIN eg_tl_Accessory acc ON tld.id = acc.tradeLicenseDetailId INNER JOIN eg_tl_TradeUnit tu ON tld.id = tu.tradeLicenseDetailId INNER JOIN eg_tl_address adr ON tld.id = adr.tradeLicenseDetailId LEFT OUTER JOIN egcl_bill eb ON  tl.applicationNumber=eb.consumercode LEFT OUTER JOIN egcl_paymentdetail epd ON eb.id=epd.billid LEFT OUTER JOIN egcl_payment ep ON ep.id=epd.paymentid WHERE tl.applicationtype = 'RENEWAL' AND tl.tenantId != 'pb.testing'",conn)
   
    newdata = pd.DataFrame(newdataquery)
    renewdata = pd.DataFrame(renewdataquery)
     
    newdata['ULB Type'] = newdata['tenantid'].map(map_MC)
    renewdata['ULB Type'] = newdata['tenantid'].map(map_MC)
 
    newdata['Ownership Subtype'] = newdata['Ownership Subtype'].map(map_ownershipsubtype)
    renewdata['Ownership Subtype'] = renewdata['Ownership Subtype'].map(map_ownershipsubtype)
    newdata['Ownership Type'] = newdata['Ownership Subtype'].map(map_ownershiptype)
    renewdata['Ownership Type'] = renewdata['Ownership Subtype'].map(map_ownershiptype)
    newdata['Application Status'] = newdata['Application Status'].map(map_status)  
    renewdata['Application Status'] = renewdata['Application Status'].map(map_status)    
 
    newdata['Structure Type'] = newdata['Structure Type'].map(map_structuretype)  
    renewdata['Structure Type'] = renewdata['Structure Type'].map(map_structuretype)    
  
    newdata[['tradetype','Trade Subtype','Trade Type Code']] = pd.DataFrame(newdata.tradetype.str.split('.').tolist(),columns = ['tradetype','Trade Subtype','Trade Type Code'])
    renewdata[['tradetype','Trade Subtype','Trade Type Code']] = pd.DataFrame(renewdata.tradetype.str.split('.').tolist(),columns = ['tradetype','Trade Subtype','Trade Type Code'])
 
    newdata = newdata.rename(columns={"Application Date": "Application_Date","Commencement Date":"Commencement_Date","Payment Date":"Payment_Date", "Valid Till":"validtill"})
    renewdata = renewdata.rename(columns={"Application Date": "Application_Date","Commencement Date":"Commencement_Date","Payment Date":"Payment_Date"})
     
    newdata['Application_Date'] = pd.to_datetime(newdata.Application_Date, format='%Y-%m-%d')
    newdata['Commencement_Date'] = pd.to_datetime(newdata.Commencement_Date, format='%Y-%m-%d')
    newdata['Payment_Date'] = pd.to_datetime(newdata.Payment_Date, format='%Y-%m-%d')
    newdata['validtill'] = pd.to_datetime(newdata.validtill, format='%Y-%m-%d')
 
    renewdata['Application_Date'] = pd.to_datetime(renewdata.Application_Date, format='%Y-%m-%d')
    renewdata['Commencement_Date'] = pd.to_datetime(renewdata.Commencement_Date, format='%Y-%m-%d')
    renewdata['Payment_Date'] = pd.to_datetime(renewdata.Payment_Date, format='%Y-%m-%d')
    
    newdata['Application_Date'] = newdata['Application_Date'].dt.strftime("%d-%m-%y")
    newdata['Commencement_Date'] = newdata['Commencement_Date'].dt.strftime("%d-%m-%y")
    newdata['Payment_Date'] = newdata['Payment_Date'].dt.strftime("%d-%m-%y")
    newdata['validtill'] = newdata['validtill'].dt.strftime("%d-%m-%y")    

    renewdata['Application_Date'] = renewdata['Application_Date'].dt.strftime("%d-%m-%y")
    renewdata['Commencement_Date'] = renewdata['Commencement_Date'].dt.strftime("%d-%m-%y")
    renewdata['Payment_Date'] = renewdata['Payment_Date'].dt.strftime("%d-%m-%y")
     
    newdata = newdata.rename(columns={"Application_Date":"Application Date" ,"Commencement_Date":"Commencement Date","Payment_Date":"Payment Date", "validtill":"Valid Till"})
    renewdata = renewdata.rename(columns={"Application_Date":"Application Date" ,"Commencement_Date":"Commencement Date","Payment_Date":"Payment Date"})
    
    newdata = newdata.drop(columns=['tenantid'])
    renewdata = renewdata.drop(columns=['tenantid'])
 
    newdata['Is Renewal Pending'] = newdata['Financial Year'].apply(lambda x: 'Yes' if date.today().month==1 | date.today().month==2 | date.today().month==3 & int(x[0:4]) + 1 <= date.today().year  else 'No')
    newdata = newdata.rename(columns={"Structure Type":"structuretype","Trade Type":"tradetype","Trade Subtype":"tradesubtype", "Accessory Category": "accessorycategory" })
    renewdata = renewdata.rename(columns={"Structure Type":"structuretype","Trade Type":"tradetype","Trade Subtype":"tradesubtype", "Accessory Category": "accessorycategory" })
 
    result = pd.merge(newdata, renewdata, how="right", on=["License Number"])
    result = result.drop_duplicates(subset = ["License Number"])
   
    result['Structure Type Modified?'] = (result.structuretype_x!=result.structuretype_y)
    result['Trade Type Modified?'] = (result.tradetype_x!=result.tradetype_y)
    result['Trade Subtype Modified?'] = (result.tradesubtype_x!=result.tradesubtype_y)
    result['Accessory Category Modified?'] = (result.accessorycategory_x!=result.accessorycategory_y)
    result['Renewed with Penanlty?'] = (result.Penalty_x!=result.Penalty_y)

    columns_to_retain=['Structure Type Modified?','Trade Type Modified?','Trade Subtype Modified?','Accessory Category Modified?','Renewed with Penanlty?','License Number']
    result = result[columns_to_retain]
    renewdata = pd.merge(renewdata,result, how="inner", on=["License Number"])
    renewdata['Trade details modified during Renewal?'] = renewdata['Structure Type Modified?'] | renewdata['Trade Type Modified?'] | renewdata['Trade Subtype Modified?'] | renewdata['Accessory Category Modified?']
    newdata['Is Renewal Pending'] = newdata['Is Renewal Pending'].map({'Yes':True,'No':False})
    newdata['temp'] = newdata['Valid Till'].apply(lambda x: True if  datetime.date(datetime.strptime(x,'%d-%m-%y')) > date.today()   else False)
    newdata['Is License Void'] = newdata['temp'] & newdata['Is Renewal Pending']

    renewdata['Structure Type Modified?'] = renewdata['Structure Type Modified?'].map({True:'Yes',False:'No'})
    renewdata['Trade Type Modified?'] = renewdata['Trade Type Modified?'].map({True:'Yes',False:'No'})
    renewdata['Trade Subtype Modified?'] = renewdata['Trade Subtype Modified?'].map({True:'Yes',False:'No'})
    renewdata['Accessory Category Modified?'] = renewdata['Accessory Category Modified?'].map({True:'Yes',False:'No'})
    renewdata['Trade details modified during Renewal?'] = renewdata['Trade details modified during Renewal?'].map({True:'Yes',False:'No'})
    renewdata['Renewed with Penanlty?'] = renewdata['Renewed with Penanlty?'].map({True:'Yes',False:'No'})
    newdata['Is License Void'] = newdata['Is License Void'].map({True:'Yes',False:'No'})
    newdata['Is Renewal Pending'] = newdata['Is Renewal Pending'].map({True:'Yes',False:'No'})
   
    newdata = newdata.rename(columns={"structuretype":"Structure Type","tradetype":"Trade Type","tradesubtype":"Trade Subtype", "accessorycategory":"Accessory Category" })
    renewdata = renewdata.rename(columns={"structuretype":"Structure Type","tradetype":"Trade Type","tradesubtype":"Trade Subtype", "accessorycategory":"Accessory Category" })

    newdata = newdata.drop(columns=['temp','Penalty'])
    
    newdata.fillna("", inplace=True)
    renewdata.fillna("", inplace=True)
    renewdata = pd.merge(renewdata,newdata[['License Number','Total Amount Paid']],on='License Number', how='left')
    renewdata = renewdata.drop_duplicates(subset = ["License Number"])
    renewdata = renewdata.rename(columns={"Total Amount Paid_y":"Old Trade license Amount","Total Amount Paid_x": "Total Amount Paid"})
     
    newdata.to_csv('/tmp/tlDatamart.csv')
    renewdata.to_csv('/tmp/tlRenewDatamart.csv')

    print("Datamart exported. Please copy it using kubectl cp command to your required location.")
    
if __name__ == '__main__':
    connect()
