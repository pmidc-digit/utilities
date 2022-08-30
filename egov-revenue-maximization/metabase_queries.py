#########################################################################################################
#Rule 1
# Number of Immovable Trades without a property ID attached

#To get the count only

{
    "intervals":["1900-01-01/2100-01-01"],
    "granularity":"all",
    "context":{
        "timeout":60000,
        "queryId":"46297453-abbe-47bc-9399-25221d56693f"
    },
    "queryType":"timeseries",
    "dataSource":"trade_license",
    "aggregations":[
        {
            "type":"count",
            "name":"count"
        }
    ],
    "filter":{
        "type":"and",
        "fields":[
            {
                "type":"not",
                "field":{
                    "type":"selector",
                    "dimension":"_source.Data.tradelicense.tradeLicenseDetail.additionalDetail.propertyId",
                    "value":null
                }
            },
            {
                "type":"not",
                "field":{
                    "type":"selector",
                    "dimension":"_source.Data.tradelicense.tradeLicenseDetail.additionalDetail.propertyId",
                    "value":null
                }
            },
            {
                "type":"like",
                "dimension":"_source.Data.tradelicense.tradeLicenseDetail.structureType",
                "pattern":"%IMMOVABLE%"
            }
        ]
    }
}




#To get the list of property IDS
{
    "intervals":["1900-01-01/2100-01-01"],
    "granularity":"all",
    "context":{
        "timeout":60000,
        "queryId":"500f5822-8f8b-4723-afc3-9643345cd5d7"
    },
    "queryType":"scan",
    "limit":1048576,
    "dataSource":"trade_license",
    "filter":{
        "type":"and",
        "fields":[
            {
                "type":"not",
                "field":{
                    "type":"selector",
                    "dimension":"_source.Data.tradelicense.tradeLicenseDetail.additionalDetail.propertyId",
                    "value":null
                }
            },
            {"type":"not",
             "field":{
                 "type":"selector",
                 "dimension":"_source.Data.tradelicense.tradeLicenseDetail.additionalDetail.propertyId",
                 "value":""
                }
            },
            {
                "type":"like",
                "dimension":"_source.Data.tradelicense.tradeLicenseDetail.structureType",
                "pattern":"%IMMOVABLE%"
            }
        ]
    },
    "columns":[
        "_source.Data.tradelicense.tradeLicenseDetail.additionalDetail.propertyId",
        "_source.Data.tradelicense.tradeName",
        "_source.Data.tradeType",
        "_source.Data.ward.name",
        "_source.Data.tenantData.city.ddrName",
        "_source.Data.tradelicense.tenantId"
    ]
}



#########################################################################################################

#Rule 2
#Number of Immovable Trades with property ID and under type residential

#To get the count only

{
    "intervals":["1900-01-01/2100-01-01"],
    "granularity":"all",
    "context":{
        "timeout":60000,
        "queryId":"5815b15c-9d4a-4ca0-a7ec-101dcbf40c6a"
    },
    "queryType":"timeseries",
    "dataSource":"trade_and_property",
    "aggregations":[
        {
            "type":"count",
            "name":"count"
        }
    ],
    "virtualColumns": [
    {
      "type": "expression",
      "name": "usageCategory",
      "expression": "upper(\"__source.Data.usageCategory\")",
      "outputType": "STRING"
    }
  ],
    "filter":{
        "type":"and",
        "fields":[
            {
                "type":"not",
                "field":{
                    "type":"selector",
                    "dimension":"_source.Data.tradelicense.tradeLicenseDetail.additionalDetail.propertyId",
                    "value":null
                }
            },
            {
                "type":"not",
                "field":{
                    "type":"selector",
                    "dimension":"_source.Data.tradelicense.tradeLicenseDetail.additionalDetail.propertyId",
                    "value":""
                }
            },
            {
                "type":"not",
                "field":{
                    "type":"selector",
                    "dimension":"usageCategory",
                    "value":"COMMERCIAL",
                    "caseSensitive":false
                }
            }
        ]
    }
}


#To get the list of property ID
{
    "intervals":["1900-01-01/2100-01-01"],
    "granularity":"all",
    "context":{
        "timeout":60000,
        "queryId":"b85c1c8d-17e1-4137-a3ff-075c04e20eeb"
    },
    "queryType":"scan",
    "limit":1048576,
    "dataSource":"trade_and_property",
    "virtualColumns": [
    {
      "type": "expression",
      "name": "usageCategory",
      "expression": "upper(\"__source.Data.usageCategory\")",
      "outputType": "STRING"
    }
  ],
    "filter":{
        "type":"and",
        "fields":[
            {
                "type":"not",
                "field":{
                    "type":"selector",
                    "dimension":"_source.Data.tradelicense.tradeLicenseDetail.additionalDetail.propertyId",
                    "value":null
                }
            },
            {
                "type":"not",
                "field":{
                    "type":"selector",
                    "dimension":"_source.Data.tradelicense.tradeLicenseDetail.additionalDetail.propertyId",
                    "value":""
                }
            },
            {
                "type":"not",
                "field":{
                    "type":"selector",
                    "dimension":"usageCategory",
                    "value":"COMMERCIAL",
                    "caseSensitive":false
                }
            }
        ]
    },
    "columns":[
        "_source.Data.tradelicense.tradeLicenseDetail.additionalDetail.propertyId",
        "_source.Data.tradelicense.tradeName",
        "_source.Data.tradeType",
        "_source.Data.usageCategory",
        "_source.Data.ward.name",
        "_source.Data.tenantData.city.ddrName",
        "_source.Data.tradelicense.tenantId"
        ]
}

#########################################################################################################

# Total Number of properties without a water connection attached to it (excluding property type 'Vacant Land')

#To get the count

{
    "intervals":["1900-01-01/2100-01-01"],
    "granularity":"all",
    "context":{
        "timeout":60000,
        "queryId":"ab4064dc-e35a-4434-900c-5022316c19ab"
    },
    "queryType":"timeseries",
    "dataSource":"rule_3",
    "aggregations":[
        {
            "type":"count",
            "name":"count"
        }
    ],
    "virtualColumns": [
    {
      "type": "expression",
      "name": "newpropertyType",
      "expression": "upper(\"_source.Data.propertyType\")",
      "outputType": "STRING"
    }
  ],
    "filter":{
        "type":"not",
        "field":{
            "type":"selector",
            "dimension":"newpropertyType",
            "value":"VACANT",
            "caseSensitive":false
        }
    }
}


#To get the list
{
    "intervals":["1900-01-01/2100-01-01"],
    "granularity":"all",
    "context":{
        "timeout":60000,
        "queryId":"aca6fdfa-26a7-413c-8fd4-e488b29149f8"
    },
    "queryType":"scan",
    "limit":1048576,
    "dataSource":"rule_3",
    "virtualColumns": [
    {
      "type": "expression",
      "name": "newpropertyType",
      "expression": "upper(\"_source.Data.propertyType\")",
      "outputType": "STRING"
    }
  ],
    "filter":{
        "type":"not",
        "field":{
            "type":"selector",
            "dimension":"newpropertyType",
            "value":"VACANT",
            "caseSensitive":false
        }
    },
    "columns":[
        "_source.Data.propertyId",
        "_source.Data.propertyType",
        "_source.Data.usageCategory",
        "_source.Data.tenantId",
        "_source.Data.ward.name",
        "_source.Data.tenantData.city.ddrName"
    ]
}



#########################################################################################################
# Total number of properties registered 

{
    "intervals":["1900-01-01/2100-01-01"],
    "granularity":"all",
    "context":{
        "timeout":60000,
        "queryId":"c7439b07-3499-4626-8262-bbae64347392"
    },
    "queryType":"timeseries",
    "dataSource":"property_service",
    "aggregations":[
        {
            "type":"count",
            "name":"count"
        }
    ],
    "filter":{
        "type":"not",
        "field":{
            "type":"selector",
            "dimension":"_source.Data.tenantId",
            "value":"pb.testing"
        }
    }
}


#########################################################################################################
# Total number of water connections

{
    "intervals":["1900-01-01/2100-01-01"],
    "granularity":"all",
    "context":{
        "timeout":60000,
        "queryId":"69567831-ad5d-40db-a2cb-85c2a6cf757b"
    },
    "queryType":"timeseries",
    "dataSource":"water_service",
    "aggregations":[
        {
            "type":"count",
            "name":"count"
        }
    ],
    "filter":{
        "type":"not",
        "field":{
            "type":"selector",
            "dimension":"_source.Data.tenantId",
            "value":"pb.testing"
        }
    }
}


#########################################################################################################

# Total collection from water module

{
    "intervals":["1900-01-01/2100-01-01"],
    "granularity":"all",
    "context":{
        "timeout":60000,
        "queryId":"34179d3e-86f8-41eb-9179-d977163d414b"
    },
    "queryType":"timeseries",
    "dataSource":"dss_service",
    "aggregations":[
        {
            "type":"longSum",
            "name":"SumOfTotalAmountPaid",
            "fieldName":"_source.dataObject.paymentDetails.totalAmountPaid"
        }
    ],
    "filter":{
        "type":"or",
        "fields":[
            {
                "type":"selector",
                "dimension":"_source.dataObject.paymentDetails.businessService",
                "value":"SW.ONE_TIME_FEE"
            },
            {
                "type":"selector",
                "dimension":"_source.dataObject.paymentDetails.businessService",
                "value":"WS.ONE_TIME_FEE"
            },
            {
                "type":"selector",
                "dimension":"_source.dataObject.paymentDetails.businessService",
                "value":"WS"
            },
            {
                "type":"selector",
                "dimension":"_source.dataObject.paymentDetails.businessService",
                "value":"SW"
            }
        ]
    }
}

#########################################################################################################
# Total collection from property module

{
    "intervals":["1900-01-01/2100-01-01"],
    "granularity":"all",
    "context":{
        "timeout":60000,
        "queryId":"9b82489f-80b1-4969-9e36-79c0f2413838"
    },
    "queryType":"timeseries",
    "dataSource":"dss_service",
    "aggregations":[
        {
            "type":"longSum",
            "name":"SumOfTotalAmountPaid",
            "fieldName":"_source.dataObject.paymentDetails.totalAmountPaid"
        }
    ],
    "filter":{
        "type":"selector",
        "dimension":"_source.dataObject.paymentDetails.businessService",
        "value":"PT"
    }
}


#########################################################################################################
# Total collection from TL module

{
    "intervals":["1900-01-01/2100-01-01"],
    "granularity":"all",
    "context":{
        "timeout":60000,
        "queryId":"9b82489f-80b1-4969-9e36-79c0f2413838"
    },
    "queryType":"timeseries",
    "dataSource":"dss_service",
    "aggregations":[
        {
            "type":"longSum",
            "name":"SumOfTotalAmountPaid",
            "fieldName":"_source.dataObject.paymentDetails.totalAmountPaid"
        }
    ],
    "filter":{
        "type":"selector",
        "dimension":"_source.dataObject.paymentDetails.businessService",
        "value":"TL"
    }
}

#########################################################################################################
#RULE 4

{
    "queryType": "topN",
    "dataSource":"dss_service",
    "virtualColumns": [],
    "dimension": {
      "type": "default",
      "dimension": "_source.dataObject.paymentDetails.bill.consumerCode",
      "outputName": "consumerCode",
      "outputType": "STRING"
    },
    "metric": {
      "type": "dimension",
      "previousStop": null,
      "ordering": {
        "type": "lexicographic"
      }
    },
    "threshold": 100000,
    "intervals":["1900-01-01/2100-01-01"],
    "granularity": {
      "type": "all"
    },
    "aggregations": [
      {
        "type": "longSum",
        "name": "totalAmountPaid",
        "fieldName": "_source.dataObject.paymentDetails.totalAmountPaid",
        "expression": null
      }
    ],
    "postAggregations": [],
    "context":{
        "timeout":60000,
        "queryId":"aca6fdfa-26a7-413c-8fd4-e488b29149f8"
    },
    "descending": false
}

#########################################################################################################


#RULE 5 - Abnormal water consumption


#To get the count

{
    "intervals":["1900-01-01/2100-01-01"],
    "granularity":"all",
    "context":{
        "timeout":60000,
        "queryId":"ab4064dc-e35a-4434-900c-5022316c19ab"
    },
    "queryType":"timeseries",
    "dataSource":"water_and_meter",
    "aggregations":[
        {
            "type":"count",
            "name":"count"
        }
    ],
    "virtualColumns": [
    {
      "type": "expression",
      "name": "newpropertyType",
      "expression": "upper(\"_source.Data.propertyUsageType\")",
      "outputType": "STRING"
    }
  ],
    "filter":{
        "type":"not",
        "field":{
            "type":"selector",
            "dimension":"newpropertyType",
            "value":"VACANT",
            "caseSensitive":false
        }
    }
}

#To get the list

{
    "intervals":["1900-01-01/2100-01-01"],
    "granularity":"all",
    "context":{
        "timeout":60000,
        "queryId":"aca6fdfa-26a7-413c-8fd4-e488b29149f8"
    },
    "queryType":"scan",
    "limit":1048576,
    "dataSource":"water_and_meter",
    "virtualColumns": [
    {
      "type": "expression",
      "name": "usageType",
      "expression": "upper(\"_source.Data.propertyId\")",
      "outputType": "STRING"
    },
    {
    "type": "expression",
    "name": "delta",
    "expression": "(((\"_source.Data.currentReading\" - \"_source.Data.lastReading\") - 1) * 100)",
    "outputType": "DOUBLE"
    },
    {
    "type": "expression",
    "name": "diff",
    "expression": "((((\"_source.Data.currentReading\" - \"_source.Data.lastReading\") - 1) * 100) > 20)",
    "outputType": "LONG"
    }
  ],
    "filter": {
      "type": "and",
      "fields": [
        {
          "type": "selector",
          "dimension": "usageType",
          "value": "PG-PT-2021-08-06-005891",
          "extractionFn": null
        },
        {
          "type": "bound",
          "dimension": "delta",
          "lower": "20000",
          "upper": null,
          "lowerStrict": true,
          "upperStrict": false,
          "extractionFn": null,
          "ordering": {
            "type": "numeric"
          }
        }
      ]
    },
    "columns":[
        "_source.Data.connectionNo",
        "_source.Data.applicationNo",
        "_source.Data.propertyId"
    ]
}

#########################################################################################################

#Collection for PT module

{
     "queryType": "timeseries",
    "dataSource":"demand",
    "virtualColumns": [],
    "dimension": {
      "type": "default",
      "dimension": "consumercode",
      "outputName": "consumerCode",
      "outputType": "STRING"
    },
    "metric": {
      "type": "dimension",
      "previousStop": null,
      "ordering": {
        "type": "lexicographic"
      }
    },
    "threshold": 100000,
    "intervals":["1900-01-01/2100-01-01"],
    "granularity": {
      "type": "all"
    },
          "filter": {
    "type": "in",
    "dimension": "businessservice",
    "values": [
      "PT"
    ]
  },
  "aggregations": [
    {
      "type": "longSum",
      "name": "Amount",
      "fieldName": "minimumamountpayable",
      "expression": null
    }
  ],
    "postAggregations": [
    {
      "type": "expression",
      "name": "p0",
      "expression": "'Collection in PT'",
      "ordering": null
    }
   ],
    "context":{
        "timeout":60000,
        "queryId":"aca6fdfa-26a7-413c-8fd4-e488b29149f8"
    },
    "descending": false
  }
  
  
#########################################################################################################

#Collection for TL module

{
     "queryType": "timeseries",
    "dataSource":"demand",
    "virtualColumns": [],
    "dimension": {
      "type": "default",
      "dimension": "consumercode",
      "outputName": "consumerCode",
      "outputType": "STRING"
    },
    "metric": {
      "type": "dimension",
      "previousStop": null,
      "ordering": {
        "type": "lexicographic"
      }
    },
    "threshold": 100000,
    "intervals":["1900-01-01/2100-01-01"],
    "granularity": {
      "type": "all"
    },
          "filter": {
    "type": "in",
    "dimension": "businessservice",
    "values": [
      "TL"
    ]
  },
  "aggregations": [
    {
      "type": "longSum",
      "name": "Amount",
      "fieldName": "minimumamountpayable",
      "expression": null
    }
  ],
    "postAggregations": [
    {
      "type": "expression",
      "name": "p0",
      "expression": "'Collection in TL'",
      "ordering": null
    }
   ],
    "context":{
        "timeout":60000,
        "queryId":"aca6fdfa-26a7-413c-8fd4-e488b29149f8"
    },
    "descending": false
  }

#########################################################################################################

#Collection for WS module
{
     "queryType": "timeseries",
    "dataSource":"demand",
    "virtualColumns": [],
    "dimension": {
      "type": "default",
      "dimension": "consumercode",
      "outputName": "consumerCode",
      "outputType": "STRING"
    },
    "metric": {
      "type": "dimension",
      "previousStop": null,
      "ordering": {
        "type": "lexicographic"
      }
    },
    "threshold": 100000,
    "intervals":["1900-01-01/2100-01-01"],
    "granularity": {
      "type": "all"
    },
          "filter": {
    "type": "in",
    "dimension": "businessservice",
    "values": [
      "WS",
      "WS.ONE_TIME_FEE"
    ]
  },
  "aggregations": [
    {
      "type": "longSum",
      "name": "Amount",
      "fieldName": "minimumamountpayable",
      "expression": null
    }
  ],
    "postAggregations": [
    {
      "type": "expression",
      "name": "p0",
      "expression": "'Collection in WS'",
      "ordering": null
    }
   ],
    "context":{
        "timeout":60000,
        "queryId":"aca6fdfa-26a7-413c-8fd4-e488b29149f8"
    },
    "descending": false
  }

#########################################################################################################
#Demand in PT

{
    "intervals":["1900-01-01/2100-01-01"],
    "granularity":"all",
    "context":{
        "timeout":60000,
        "queryId":"9b82489f-80b1-4969-9e36-79c0f2413838"
    },
    "queryType":"timeseries",
      "dataSource": {
    "type": "table",
    "name": "collection"
    },
  "aggregations": [
    {
      "type": "longSum",
      "name": "Amount",
      "fieldName": "_source.dataObject.paymentDetails.totalAmountPaid",
      "expression": null
    }
  ],
   "postAggregations": [
    {
      "type": "expression",
      "name": "p0",
      "expression": "'Demand in PT'",
      "ordering": null
    }
   ],
    "filter": {
        "type": "selector",
        "dimension": "_source.dataObject.paymentDetails.businessService",
        "value": "PT",
        "extractionFn": null
      }
}


#########################################################################################################
#Demand in TL
{
    "intervals":["1900-01-01/2100-01-01"],
    "granularity":"all",
    "context":{
        "timeout":60000,
        "queryId":"9b82489f-80b1-4969-9e36-79c0f2413838"
    },
    "queryType":"timeseries",
      "dataSource": {
    "type": "table",
    "name": "collection"
    },
  "aggregations": [
    {
      "type": "longSum",
      "name": "Amount",
      "fieldName": "_source.dataObject.paymentDetails.totalAmountPaid",
      "expression": null
    }
  ],
   "postAggregations": [
    {
      "type": "expression",
      "name": "p0",
      "expression": "'Demand in TL'",
      "ordering": null
    }
   ],
    "filter": {
        "type": "selector",
        "dimension": "_source.dataObject.paymentDetails.businessService",
        "value": "TL",
        "extractionFn": null
      }
}


#########################################################################################################
#Demand in WS

{
    "intervals":["1900-01-01/2100-01-01"],
    "granularity":"all",
    "context":{
        "timeout":60000,
        "queryId":"9b82489f-80b1-4969-9e36-79c0f2413838"
    },
    "queryType":"timeseries",
      "dataSource": {
    "type": "table",
    "name": "collection"
    },
  "aggregations": [
    {
      "type": "longSum",
      "name": "Amount",
      "fieldName": "_source.dataObject.paymentDetails.totalAmountPaid",
      "expression": null
    }
  ],
   "postAggregations": [
    {
      "type": "expression",
      "name": "p0",
      "expression": "'Demand in WS'",
      "ordering": null
    }
   ],
   "filter": {
    "type": "in",
    "dimension": "_source.dataObject.paymentDetails.businessService",
    "values": [
      "WS",
      "WS.ONE_TIME_FEE"
    ]
  },
  "granularity": {
    "type": "all"
  }
}
#########################################################################################################
#Total trades registered

{
    "intervals":["1900-01-01/2100-01-01"],
    "granularity":"all",
    "context":{
        "timeout":60000,
        "queryId":"69567831-ad5d-40db-a2cb-85c2a6cf757b"
    },
    "queryType":"timeseries",
    "dataSource":"trade_license",
  "aggregations": [
    {
      "type": "count",
      "name": "count"
    }
  ],
       "postAggregations": [
    {
      "type": "expression",
      "name": "p0",
      "expression": "'Total trades registered'",
      "ordering": null
    }
   ]
}
#########################################################################################################



