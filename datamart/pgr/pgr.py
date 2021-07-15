import psycopg2
import csv
import pandas as pd
import numpy as np

def map_CompSubtype(s):
    if s == 'NoStreetlight':
        return 'No Street Light'
    elif s == 'StreetLightNotWorking':
        return 'Street Light Not Working'
    elif s == 'GarbageNeedsTobeCleared':
        return 'Garbage Needs To Be Cleared'
    elif s == 'DamagedGarbageBin':
        return 'Damaged Garbage Bin'
    elif s == 'BurningOfGarbage':
        return 'Burning Of Garbage'
    elif s == 'illegalDischargeOfSewage':
        return 'Illegal Discharge Of Sewage'
    elif s == 'OverflowingOrBlockedDrain':
        return 'Overflowing Or Blocked Drain'
    elif s == 'BlockOrOverflowingSewage':
        return 'Block Or Overflowing Sewage'
    elif s == 'ShortageOfWater':
        return 'Shortage Of Water'
    elif s == 'NoWaterSupply':
        return 'No Water Supply'
    elif s == 'DirtyWaterSupply':
        return 'Dirty Water Supply'
    elif s == 'BrokenWaterPipeOrLeakage':
        return 'Broken Water Pipe Or Leakage'
    elif s == 'WaterPressureisVeryLess':
        return 'Water Pressure Is Very Less' 
    elif s == 'WaterLoggedRoad':
        return 'Water Logged Road'
    elif s == 'Manhole Cover Missing Or Damaged':
        return 'Manhole Cover Missing Or Damaged'
    elif s == 'DamagedOrBlockedFootpath':
        return 'Damaged Or Blocked Footpath'
    elif s == 'ConstructionMaterialLyingOntheRoad':
        return 'Construction Material Lying On The Road'
    elif s == 'RequestSprayingOrFoggingOperation':
        return 'Request Spraying Or Fogging Operation'
    elif s == 'DeadAnimals':
        return 'Dead Animals'
    elif s == 'StrayAnimals':
        return 'Stray Animals'
    elif s == 'PublicToiletIsDamaged':
        return 'Public Toilet Is Damaged'
    elif s == 'NoWaterOrElectricityinPublicToilet':
        return 'No Water Or Electricity In Public Toilet'
    elif s == 'IllegalShopsOnFootPath':
        return 'Illegal Shops On Footpath'
    elif s == 'IllegalConstructions':
        return 'Illegal Constructions'
    elif s == 'IllegalParking':
        return 'Illegal Parking'
    elif s == 'IllegalCuttingOfTrees':
        return 'Illegal Cutting Of Trees'
    elif s == 'CuttingOrTrimmingOfTreeRequired':
        return 'Cutting Or Trimming Of Tree Required'
    elif s == 'OpenDefecation':
        return 'Open Defecation'
    elif s == 'ParkRequiresMaintenance':
        return 'Park Requires Maintenance'
    elif s == 'Others':
        return 'Others'
    elif s == 'eGovSewerage':
        return 'eGov Sewerage'
    elif s == 'RequestForFumigation':
        return 'Request For Fumigation'
    elif s == 'CleaningOfDrains':
        return 'Cleaning Of Drains'
    elif s == 'RegardingDatesOfDepositionOfPropertyTax':
        return 'Regarding Dates Of Deposition Of Property Tax'
    elif s == 'CleaningOfRoadGullies':
        return 'Cleaning Of Road Gullies'
    elif s == 'EncroachmentOnAnyKindOnGovernmentLand':
        return 'Encroachment On Any Kind On Government Land'
    elif s == 'BirthAndDeathCertificates':
        return 'Birth And Death Certificates'
    elif s == 'TownPlanning':
        return 'Town Planning'
    elif s == 'eGovPropertyTax':
        return 'eGov Property Tax'
    elif s == 'Excessive Ramps Of Shops And Houses':
        return 'Excessive Ramps Of Shops And Houses'
    elif s == 'RequestForConstructionOfNewRoadsFootpathsPavements':
        return 'Request For Construction Of New Roads Footpaths Pavements'
    elif s == 'AgainstEmployee':
        return 'Against Employee'
    elif s == 'ImproperTimingOfWaterSupply':
        return 'Improper Timing Of Water Supply'
    elif s == 'MaintenanceOfLightsOfAllParksGreenBelts':
        return 'Maintenance Of Lights Of All Parks Green Belts'
    elif s == 'eGovtradeLicense':
        return 'eGov Trade License'
    elif s == 'HowToPayPT':
        return 'How To Pay PT'
    elif s == 'WrongCalculationPT':
        return 'Wrong Calculation PT'
    elif s == 'Receipt Not Generated':
        return 'Receipt Not Generated'
    elif s == 'others':
        return 'others'
    elif s == 'DamagedRoad':
        return 'Damaged Road'
    elif s == 'DirtyOrSmellyPublicToilets':
        return 'Dirty Or Smelly Public Toilets'
    elif s == 'WaterEnteredHouseRainySeason':
        return 'Water Entered House Rainy Season'
    elif s == 'InstallationOfNewStreetLight':
        return 'Installation Of New Street Light'
    elif s == 'Non Sweeping Of Road':
        return 'Non Sweeping Of Road' 
    elif s == 'CongressGrassCutting':
        return 'Congress Grass Cutting'
    elif s == 'SewageMainHoleCoverMissingOrBroken':
        return 'Sewage Main Hole Cover Missing Or Broken'
    elif s == 'SewerageManholeCoverRaising':
        return 'Sewerage Manhole Cover Raising'
    elif s == 'CleaningOfSewageRemoveSLURRYGaar':
        return 'Cleaning Of Sewage Remove Slurry Gaar'
    elif s == 'IllegalRehriesOnRoad':
        return 'Illegal Rehries On Road'
    elif s == 'RoadJalliBroken':
        return 'Road Jalli Broken' 
    

def map_CompType(s):
    Streetlights = ['No Street Light','Street Light Not Working', 'Installation Of New Street Light']

    Garbage = ['Garbage Needs To Be Cleared','Damaged Garbage Bin','Burning Of Garbage']

    WaterAndSewerage = ['Illegal Discharge Of Sewage',
                    'Overflowing Or Blocked Drain',
                    'Block Or Overflowing Sewage',
                    'Shortage Of Water',
                    'No Water Supply',
                    'Dirty Water Supply',
                    'Broken Water Pipe Or Leakage',
                    'Water Pressure Is Very Less',
                    'eGov Sewerage',
                    'Improper Timing Of Water Supply','Sewerage Manhole Cover Raising'
                    ,'Cleaning Of Sewage Remove Slurry Gaar'
                    , 'Sewage Main Hole Cover Missing Or Broken'
                   ]

    Drains = ['Cleaning Of Drains','Water Entered House Rainy Season'  ]

    RoadsAndFootpaths = ['Water Logged Road',
                     'Manhole Cover Missing Or Damaged',
                     'Damaged Or Blocked Footpath',
                    'Construction Material Lying On The Road',
                     'Cleaning Of Road Gullies',
                     'Damaged Road',
                     'Non Sweeping Of Road' ,
                     'Illegal Rehries On Road',
                     'Road Jalli Broken' ,
                     'Request For Construction Of New Roads Footpaths Pavements'
                    ]

    Mosquitos = ['Request Spraying Or Fogging Operation']

    Animals = [ 'Dead Animals',
           'Stray Animals'
          ]

    PublicToilets = ['Public Toilet Is Damaged',
                 'Dirty Or Smelly Public Toilets',
                 'No Water Or Electricity In Public Toilet'
                ]

    LandViolations = ['Illegal Shops On Footpath',
                  'Illegal Constructions',
                   'Illegal Parking'   ]

    Trees = ['Illegal Cutting Of Trees',
        'Cutting Or Trimming Of Tree Required']

    OpenDefecation = ['Open Defecation']

    Parks = ['Park Requires Maintenance','Maintenance Of Lights Of All Parks Green Belts']

    Others = ['Others', 'Request For Fumigation','Encroachment On Any Kind On Government Land',
         'Birth And Death Certificates','Congress Grass Cutting', 'Against Employee','Town Planning','eGov Trade License','Excessive Ramps Of Shops And Houses']

    PropertyTaxHouseTax = ['others', 'Regarding Dates Of Deposition Of Property Tax'
                       ,'eGov Property Tax','How To Pay PT',
                       'Wrong Calculation PT',
                       'Receipt Not Generated',
                      ]

    if s in Streetlights:
        return 'Streetlights'
    elif s in Garbage:
        return 'Garbage'
    elif s in WaterAndSewerage:
        return 'Water And Sewerage'
    elif s in RoadsAndFootpaths:
        return 'Roads And Footpaths'
    elif s in Mosquitos:
        return 'Mosquitos'
    elif s in Animals:
        return 'Animals'
    elif s in PublicToilets:
        return 'Public Toilets'
    elif s in LandViolations:
        return 'Land Violations'
    elif s in Trees:
        return 'Trees'
    elif s in OpenDefecation:
        return 'Open Defecation'
    elif s in Parks:
        return 'Parks'
    elif s in Others:
        return 'Others'
    elif s in Drains:
        return 'Drains'
    elif s in PropertyTaxHouseTax:
        return 'Property Tax/House Tax'
    
    
def connect(): 
    try:
        conn = psycopg2.connect(database="{{REPLACE-WITH-DATABASE}}", user="{{REPLACE-WITH-USERNAME}}", password="{{REPLACE-WITH-PASSWORD}}", host="{{REPLACE-WITH-HOST}}", port="5432")
        print("Connection established!")
   
    except Exception as exception:
        print("Exception occurred while connecting to the database")
        print(exception)

    sqlquery = pd.read_sql_query("SELECT DISTINCT(srv.servicerequestid) AS \"Service ID\", srv.servicecode AS \"Complaint Subtype\", INITCAP(srv.status) AS \"Status\", INITCAP(srv.rating) AS \"Rating\",  INITCAP(srv.source) AS \"Source\", INITCAP( SUBSTRING(adr.city, 4)) AS \"City\" FROM eg_pgr_service srv INNER JOIN eg_pgr_action act ON srv.servicerequestid = act.businesskey INNER JOIN eg_pgr_address adr ON srv.addressid = adr.uuid WHERE active != 'f' AND srv.tenantid != 'pb.testing'",conn)
    pgrgen = pd.DataFrame(sqlquery)
    pgrgen['Complaint Subtype'] = pgrgen['Complaint Subtype'].map(map_CompSubtype)
    pgrgen['Complaint Type'] = pgrgen['Complaint Subtype'].map(map_CompType)
    pgrgen.fillna('', inplace=True)
    pgrgen.to_csv('/home/priyanka/Desktop/pgrDatamart.csv')
    print("Datamart exported. Please copy it using kubectl cp command to you required location.")

if __name__ == '__main__':
    connect()
