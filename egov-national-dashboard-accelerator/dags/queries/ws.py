
def extract_ws_collection_by_payment_channel_type(metrics, region_bucket):
  groupby_usage = []
  groupby_channel = []
  collection = []

  if region_bucket.get('byChannel'):
    channel_buckets = region_bucket.get('byChannel').get('buckets')
    for channel_bucket in channel_buckets:
      channel = channel_bucket.get('key')
      value = channel_bucket.get('byChannel').get('value') if channel_bucket.get('byChannel') else 0
      groupby_channel.append({ 'name' : channel, 'value' : value})
  
  if region_bucket.get('byUsageType'):
    usage_type_buckets = region_bucket.get('byUsageType').get('buckets')
    for usage_type_bucket in usage_type_buckets:
      usage_type = usage_type_bucket.get('key')
      value = usage_type_bucket.get('byUsageType').get('value') if usage_type_bucket.get('byUsageType') else 0
      groupby_usage.append({ 'name' : usage_type, 'value' : value})
  

  collection.append({ 'groupBy': 'usageType', 'buckets' : groupby_usage})
  collection.append({ 'groupBy': 'paymentChannelType', 'buckets' : groupby_channel})
  metrics['todaysCollection'] = collection
  
  
  return metrics


ws_collection_by_payment_channel_type = {'path': 'receipts-consumers/_search',
                                 'name': 'ws_collection_by_payment_channel_type',
                                 'lambda': extract_ws_collection_by_payment_channel_type,
                                 'query':
                                 """
{{
"size":0,
"query": {{
        "bool": {{
              "must_not": [
                {{
                  "term": {{
                    "status": "Cancelled"
                  }}
                }}
              ],
          "must": [
            {{
              "range": {{
                "receiptdate": {{
                "gte": {0},
                "lte": {1},
                "format": "epoch_millis"
                }}
              }}
            }}
          ]
            }}
       }},
    "aggs": {{
            "ward": {{
              "terms": {{
                "field": "block.keyword"
              }},
        "aggs": {{
            "ulb": {{
              "terms": {{
                "field": "cityname.keyword"
              }},
              "aggs": {{
                "region": {{
                  "terms": {{
                    "field": "regionname.keyword"
                     }},
                   "aggs": {{
                      "byUsageType": {{
                        "terms": {{
                          "field": "consumertype.keyword"
                        }},
                          "aggs": {{
                              "byUsageType": {{
                                "sum":{{
                                "field": "totalamount"
                              }}
                            }}
                          }}
                         }},
                      "byChannel": {{
                        "terms": {{
                          "field": "channel.keyword"
                        }},
                          "aggs": {{
                            "byChannel": {{
                              "sum": {{
                                "field": "totalamount"
                              }}
                            }}
                          }}
                         }}
                  
                }}
}}
}}
}}
}}
}}
}}
}}

"""
                                 }


def extract_ws_collection_by_tax_head_connection_type(metrics, region_bucket):
  groupby_tax_heads = []
  collection = metrics.get('todaysCollection') if metrics.get('todaysCollection') else []

  if region_bucket.get('collectionbytaxHeads'):
    tax_head_buckets = region_bucket.get('collectionbytaxHeads').get('buckets')
    if tax_head_buckets:
      for tax_head_bucket in tax_head_buckets:
        tax_head = tax_head_bucket.get('key')
        value = tax_head_bucket.get('bytaxHeads').get('value') if tax_head_bucket.get('bytaxHeads') else 0
        groupby_tax_heads.append({ 'name' : tax_head, 'value' : value})
    
 
  collection.append({ 'groupBy': 'taxHeads', 'buckets' : groupby_tax_heads})
  metrics['todaysCollection'] = collection
  
  
  return metrics

ws_collection_by_tax_head_connection_type = {
    'path': 'receipts-consumers/_search',
    'name': 'ws_collection_by_tax_head_connection_type',
    'lambda': extract_ws_collection_by_tax_head_connection_type,
    'query': """
{{
  "size":0,
  "query": {{
        "bool": {{
              "must_not": [
                {{
                  "term": {{
                    "status": "Cancelled"
                  }}
                }}
              ],
              "must":[
                {{
                   "range": {{
                      "receiptdate": {{
                      "gte": {0},
                      "lte":  {1},
                      "format": "epoch_millis"
                }}
              }}
                }}
                ]
            }}
}},
  "aggs": {{
      "ward": {{
        "terms": {{
          "field": "block.keyword"
        }},
        "aggs": {{
            "ulb": {{
              "terms": {{
                "field": "cityname.keyword"
              }},
              "aggs": {{
                "region": {{
                  "terms": {{
                    "field": "regionname.keyword"
                     }},
  "aggs": {{
    "collectionbytaxHeads": {{
      "filter": {{
        "bool": {{
          "must_not": [
            {{
              "term": {{
                "status": "Cancelled"
              }}
            }}
          ]
        }}
      }},
      "aggs": {{
        "bytaxHeads": {{
          "sum": {{
            "field": "totalamount"
          }}
        }}
      }}
    }}
  }}
}}
}}
}}
}}
}}
}}
}}


"""
}


def extract_ws_pending_connections(metrics, region_bucket):
  groupby_duration = []
  collection = []
  duration_buckets = ['0to3Days', '3to7Days', '7to15Days', 'MoreThan15Days' ]
  for duration_bucket in duration_buckets:
    if region_bucket.get(duration_bucket):
      inner_buckets = region_bucket.get(duration_bucket).get('buckets')
      value = inner_buckets[0].get('doc_count') if inner_buckets and len(inner_buckets) > 0 else 0
      groupby_duration.append({ 'name' : duration_bucket, 'value' : value})

  collection.append({ 'groupBy': 'duration', 'buckets' : groupby_duration})
  metrics['pendingConnections'] = collection
  
  
  return metrics


ws_pending_connections = {'path': 'wsapplications/_search',
                              'name': 'ws_pending_connections',
                              'lambda': extract_ws_pending_connections,
                              'query': """

{{
  "size": 0,
    "query": {{
        "bool": {{
          "must": [
            {{
              "terms": {{
                "servicetype.keyword": [
                  "Water Charges",
                  "Sewerage Charges"
                ]
              }}
            }},
            {{
              "terms": {{
                "applicationstatus.keyword": [
                  "Created",
                  "Rejected",
                  "Verified",
                  "verified"
                ]
              }}
            }},
            {{
                   "range": {{
                      "createddate": {{
                      "gte": {0},
                      "lte": {1},
                      "format": "epoch_millis"
                }}
              }}
            }}
          ]
        }}
        }},
            "aggs": {{
              "ward": {{
                "terms": {{
                  "field": "block.keyword"
                }},
              "aggs": {{
                "ulb": {{
                  "terms": {{
                    "field": "cityname.keyword"
                  }},
                "aggs": {{
                  "region": {{
                    "terms": {{
                      "field": "regionname.keyword"
                    }},
                    "aggs": {{
        "0to3Days": {{
          "date_range": {{
            "field": "applicationdate",
            "ranges": [
              {{
                "from": "now-3d/d",
                "to": "now"
              }}
            ]
          }}
        }},
        "3to7Days": {{
          "date_range": {{
            "field": "applicationdate",
            "ranges": [
              {{
                "from": "now-1w",
                "to": "now-3d/d"
              }}
            ]
          }}
        }},
        "7to15Days": {{
          "date_range": {{
            "field": "applicationdate",
            "ranges": [
              {{
                "from": "now-15d",
                "to": "now-1w"
              }}
            ]
          }}
        }},
        "MoreThan15Days": {{
          "date_range": {{
            "field": "applicationdate",
            "ranges": [
              {{
                "from": "now-2y",
                "to": "now-15d"
              }}
            ]
          }}
        }}

                  }}
    }}
  }}
}}
}}
}}
}}
}}


	
"""
                              }


def extract_ws_sewerage_connections(metrics, region_bucket):
  groupby_usage = []
  groupby_channel = []
  collection =  []

  if region_bucket.get('sewerageConnectionsbyChannelType'):
    channel_buckets = region_bucket.get('sewerageConnectionsbyChannelType').get('buckets')
    for channel_bucket in channel_buckets:
      channel = channel_bucket.get('key')
      value = channel_bucket.get('sewerageConnectionsbyChannelType').get('value') if channel_bucket.get('sewerageConnectionsbyChannelType') else 0
      groupby_channel.append({ 'name' : channel, 'value' : value})
  
  if region_bucket.get('sewerageConnectionsbyUsageType'):
    usage_type_buckets = region_bucket.get('sewerageConnectionsbyUsageType').get('buckets')
    for usage_type_bucket in usage_type_buckets:
      usage_type = usage_type_bucket.get('key')
      value = usage_type_bucket.get('sewerageConnectionsbyUsageType').get('value') if usage_type_bucket.get('sewerageConnectionsbyUsageType') else 0
      groupby_usage.append({ 'name' : usage_type, 'value' : value})
  

  collection.append({ 'groupBy': 'usageType', 'buckets' : groupby_usage})
  collection.append({ 'groupBy': 'channelType', 'buckets' : groupby_channel})
  metrics['sewerageConnections'] = collection
  
  
  return metrics


ws_sewerage_connections = {'path': 'wsapplications/_search',
                     'name': 'ws_sewerage_connections',
                     'lambda': extract_ws_sewerage_connections,

                     'query': """
{{
  "size": 0,
    "query": {{
        "bool": {{
          "must_not": [
            {{
              "term": {{
                "applicationstatus.keyword": "Cancelled"
              }}
            }}
          ],
          "must": [
            {{
              "terms": {{
                "servicetype.keyword": [
                  "Sewerage Charges"
                ]
              }}
            }},
            {{
              "terms": {{
                "connectionstatus.keyword": [
                  "ACTIVE"
                ]
              }}
            }},
            {{
                   "range": {{
                      "createddate": {{
                      "gte": {0},
                      "lte": {1},
                      "format": "epoch_millis"
                }}
              }}
            }}
          ]
            }}
      }},
    "aggs": {{
              "ward": {{
                "terms": {{
                  "field": "block.keyword"
                }},
            "aggs": {{
              "ulb": {{
                "terms": {{
                  "field": "cityname.keyword"
                }},
              "aggs": {{
                "region": {{
                  "terms": {{
                    "field": "regionname.keyword"
                  }},
                  "aggs": {{
                    "sewerageConnectionsbyChannelType": {{
                      "terms": {{
                        "field": "channel.keyword"
                      }},
                      "aggs": {{
                        "sewerageConnectionsbyChannelType": {{
                          "value_count": {{
                            "field": "applicationnumber.keyword"
                          }}
                        }}
                      }}
                    }},
                    "sewerageConnectionsbyUsageType": {{
                      "terms": {{
                        "field": "usage.keyword"
                      }},
                    "aggs": {{
                        "sewerageConnectionsbyUsageType": {{
                          "value_count": {{
                            "field": "applicationnumber.keyword"
                          }}
                        }}
                      }}
                  }}
    }}
  }}
}}
}}
}}
}}
}}
}}



"""
                     }


def extract_ws_water_connections(metrics, region_bucket):
  groupby_usage = []
  groupby_channel = []
  groupby_meter = []
  collection =  []

  if region_bucket.get('waterConnectionsbyChannelType'):
    channel_buckets = region_bucket.get('waterConnectionsbyChannelType').get('buckets')
    for channel_bucket in channel_buckets:
      channel = channel_bucket.get('key')
      value = channel_bucket.get('waterConnectionsbyChannelType').get('value') if channel_bucket.get('waterConnectionsbyChannelType') else 0
      groupby_channel.append({ 'name' : channel, 'value' : value})
  
  if region_bucket.get('waterConnectionsbyUsageType'):
    usage_type_buckets = region_bucket.get('waterConnectionsbyUsageType').get('buckets')
    for usage_type_bucket in usage_type_buckets:
      usage_type = usage_type_bucket.get('key')
      value = usage_type_bucket.get('waterConnectionsbyUsageType').get('value') if usage_type_bucket.get('waterConnectionsbyUsageType') else 0
      groupby_usage.append({ 'name' : usage_type, 'value' : value})
  
  if region_bucket.get('waterConnectionsbyMeterType'):
    meter_type_buckets = region_bucket.get('waterConnectionsbyMeterType').get('buckets')
    for meter_type_bucket in meter_type_buckets:
      meter_type = meter_type_bucket.get('key')
      value = meter_type_bucket.get('waterConnectionsbyMeterType').get('value') if meter_type_bucket.get('sewerageConnectionsbyUsageType') else 0
      groupby_meter.append({ 'name' : meter_type, 'value' : value})
  

  collection.append({ 'groupBy': 'usageType', 'buckets' : groupby_usage})
  collection.append({ 'groupBy': 'channelType', 'buckets' : groupby_channel})
  collection.append({ 'groupBy': 'meterType', 'buckets' : groupby_channel})
  metrics['waterConnections'] = collection
  
  
  return metrics


ws_water_connections = {'path': 'wsapplications/_search',
                            'name': 'ws_water_connections',
                            'lambda': extract_ws_water_connections,
                            'query': """

 {{
    "size": 0,
      "query": {{
          "bool": {{
            "must_not": [
              {{
                "term": {{
                  "applicationstatus.keyword": "Cancelled"
                }}
              }}
            ],
            "must": [
              {{
                "terms": {{
                  "servicetype.keyword": [
                    "Water Charges"
                  ]
                }}
              }},
              {{
                "terms": {{
                  "connectionstatus.keyword": [
                    "ACTIVE"
                  ]
                }}
              }},
              {{
                   "range": {{
                      "createddate": {{
                      "gte": {0},
                      "lte": {1},
                      "format": "epoch_millis"
                }}
              }}
              }}
            ]
          }}
        }},
            "aggs": {{
                "ward": {{
                  "terms": {{
                    "field": "block.keyword"
                  }},
              "aggs": {{
                "ulb": {{
                  "terms": {{
                    "field": "cityname.keyword"
                  }},
                "aggs": {{
                  "region": {{
                    "terms": {{
                      "field": "regionname.keyword"
                    }},
                    "aggs": {{
                      "waterConnectionsbyChannelType": {{
                        "terms": {{
                          "field": "channel.keyword"
                        }},
                        "aggs": {{
                          "waterConnectionsbyChannelType": {{
                            "value_count": {{
                              "field": "applicationnumber.keyword"
                            }}
                          }}
                        }}
                      }},
                      "waterConnectionsbyUsageType": {{
                        "terms": {{
                          "field": "usage.keyword"
                        }},
                      "aggs": {{
                          "waterConnectionsbyUsageType": {{
                            "value_count": {{
                              "field": "applicationnumber.keyword"
                            }}
                          }}
                        }}
                    }},
                    "waterConnectionsbyMeterType": {{
                        "terms": {{
                          "field": "connectiontype.keyword"
                        }},
                        "aggs": {{
                          "waterConnectionsbyMeterType": {{
                            "value_count": {{
                              "field": "applicationnumber.keyword"
                            }}
                          }}
                        }}
                      }}
                    }}
    }}
  }}
  }}
  }}
  }}
  }}
  }}




"""
                            }


def extract_ws_todays_applications(metrics, region_bucket):
    metrics['todaysTotalApplications'] = region_bucket.get('todaysTotalApplications').get(
        'value') if region_bucket.get('todaysTotalApplications') else 0
    return metrics


ws_todays_applications = {'path': 'wsapplications/_search',
                         'name': 'ws_todays_applications',
                         'lambda': extract_ws_todays_applications,
                         'query': """

 {{
    "size": 0,
    "query":{{
      "bool": {{
        "must": [
          {{
             "range": {{
                "createddate": {{
                "gte": {0},
                "lte": {1},
                "format": "epoch_millis"
                }}
          }}
          }}
        ]
      }}
    }},
        "aggs": {{
            "ward": {{
              "terms": {{
                "field": "block.keyword"
              }},
          "aggs": {{
            "ulb": {{
              "terms": {{
                "field": "cityname.keyword"
              }},
            "aggs": {{
              "region": {{
                "terms": {{
                  "field": "regionname.keyword"
                }},
                "aggs": {{
                  "todaysTotalApplications": {{
                    "value_count": {{
                      "field": "applicationnumber.keyword"
                    }}
                }}
                }}
                }}
                }}
                }}
                }}
                }}
                }}
                }}


"""
                         }


def extract_ws_closed_applications(metrics, region_bucket):
    metrics['todaysClosedApplications'] = region_bucket.get('todaysClosedApplications').get(
        'value') if region_bucket.get('todaysClosedApplications') else 0
    return metrics


ws_closed_applications = {'path': 'wsapplications/_search',
                         'name': 'ws_closed_applications',
                         'lambda': extract_ws_closed_applications,
                         'query': """

 {{
    "size": 0,
          "query": {{
          "bool": {{
            "must_not": [
              {{
                "term": {{
                  "applicationstatus.keyword": "Closed"
                }}
              }}
            ],
            "must": [
              {{
                "range": {{
                "createddate": {{
                "gte": {0},
                "lte": {1},
                "format": "epoch_millis"
                }}
          }}
              }}
            ]
          }}
        }},
            "aggs": {{
                "ward": {{
                  "terms": {{
                    "field": "block.keyword"
                  }},
              "aggs": {{
                "ulb": {{
                  "terms": {{
                    "field": "cityname.keyword"
                  }},
                "aggs": {{
                  "region": {{
                    "terms": {{
                      "field": "regionname.keyword"
                    }},
                    "aggs": {{
                      "todaysClosedApplications": {{
                        "value_count": {{
                          "field": "applicationnumber.keyword"
                        }}
                    }}
    }}
    }}
    }}
    }}
    }}
    }}
    }}
    }}


"""
}





def extract_ws_connections_created_by_connection_type(metrics, region_bucket):
  groupby_connection_type = []
  collection = metrics.get('connectionsCreated') if metrics.get('connectionsCreated') else []

  if region_bucket.get('meteredconnectionCreated'):
    created_buckets = region_bucket.get('meteredconnectionCreated').get('meteredconnectionCreated')
    if created_buckets:
      groupby_connection_type.append({'name' : 'WATER.METERED', 'value' : created_buckets.get('value') if created_buckets else 0})
  
  if region_bucket.get('sewerageconnectionCreated'):
    created_buckets = region_bucket.get('sewerageconnectionCreated').get('sewerageconnectionCreated')
    if created_buckets:
      groupby_connection_type.append({'name' : 'SEWERAGE', 'value' : created_buckets.get('value') if created_buckets else 0})
  
  if region_bucket.get('non-meteredconnectionCreated'):
    created_buckets = region_bucket.get('non-meteredconnectionCreated').get('non-meteredconnectionCreated')
    if created_buckets:
      groupby_connection_type.append({'name' : 'WATER.NONMETERED', 'value' : created_buckets.get('value') if created_buckets else 0})
  
  collection.append({ 'groupBy': 'connectionType', 'buckets' : groupby_connection_type})
  metrics['connectionsCreated'] = collection

ws_connections_created_by_connection_type = {'path': 'wsapplications/_search',
                         'name': 'ws_connections_created_by_connection_type',
                         'lambda': extract_ws_connections_created_by_connection_type,
                         'query': """
 {{
    "size": 0,
      "query": {{
          "bool": {{
            "must": [
              {{
                "terms": {{
                  "servicetype.keyword": [
                    "Water Charges",
                     "Sewerage Charges"
                  ]
                }}
              }},
                {{
                "range": {{
                  "createddate": {{
                  "gte": {0},
                  "lte": {1},
                  "format": "epoch_millis"
                }}
                }}
              }}
              ]
          }}
          }},
            "aggs": {{
                "ward": {{
                  "terms": {{
                    "field": "block.keyword"
                  }},
              "aggs": {{
                "ulb": {{
                  "terms": {{
                    "field": "cityname.keyword"
                  }},
                "aggs": {{
                  "region": {{
                    "terms": {{
                      "field": "regionname.keyword"
                    }},
                    "aggs": {{
                      "meteredconnectionCreated": {{
                        "filter": {{
                          "term": {{
                            "connectiontype.keyword": "Metered"
                          }}
                        }},
                        "aggs": {{
                          "meteredconnectionCreated": {{
                            "value_count": {{
                              "field": "applicationnumber.keyword"
                            }}
                          }}
                        }}
                      }},
                      "non-meteredconnectionCreated": {{
                        "filter": {{
                            "term" : {{ "connectiontype.keyword" : "Non-Metered" }}
                             }},
                              "aggs": {{
                                "non-meteredconnectionCreated": {{
                                  "value_count": {{
                                    "field": "applicationnumber.keyword"
                                  }}
                                }}
                              }}
                      }},
                      "sewerageconnectionCreated": {{
                        "filter": {{
                          "term": {{
                            "servicetype.keyword": "Sewerage Charges"
                          }}
                        }},
                        "aggs": {{
                          "sewerageconnectionCreated": {{
                            "value_count": {{
                              "field": "applicationnumber.keyword"
                            }}
                          }}
                        }}
                      }}
                    }}
                    }}
                    }}
                    }}
                    }}
                    }}
                    }}
                    }}



"""
}


def extract_ws_connections_created_by_channel_type(metrics, region_bucket):
  groupby_channel_type = []
  collection = metrics.get('connectionsCreated') if metrics.get('connectionsCreated') else []

  if region_bucket.get('meteredconnectionCreated'):
    created_buckets = region_bucket.get('meteredconnectionCreated').get('meteredconnectionCreated')
    if created_buckets:
      groupby_channel_type.append({'name' : 'WATER.METERED', 'value' : created_buckets.get('value') if created_buckets else 0})
  
  if region_bucket.get('sewerageconnectionCreated'):
    created_buckets = region_bucket.get('sewerageconnectionCreated').get('sewerageconnectionCreated')
    if created_buckets:
      groupby_channel_type.append({'name' : 'SEWERAGE', 'value' : created_buckets.get('value') if created_buckets else 0})
  
  if region_bucket.get('non-meteredconnectionCreated'):
    created_buckets = region_bucket.get('non-meteredconnectionCreated').get('non-meteredconnectionCreated')
    if created_buckets:
      groupby_channel_type.append({'name' : 'WATER.NONMETERED', 'value' : created_buckets.get('value') if created_buckets else 0})
  
  collection.append({ 'groupBy': 'connectionType', 'buckets' : groupby_channel_type})
  metrics['connectionsCreated'] = collection


ws_connections_created_by_channel_type = {'path': 'wsapplications/_search',
                         'name': 'ws_connections_created_by_channel_type',
                         'lambda': extract_ws_connections_created_by_channel_type,
                         'query': """
  {{
    "size": 0,
      "query": {{
          "bool": {{
            "must_not": [
              {{
                "term": {{
                  "applicationstatus.keyword": "Cancelled"
                }}
              }}
            ],
            "must": [
              {{
                "terms": {{
                  "servicetype.keyword": [
                    "Water Charges"
                  ]
                }}
              }},
              {{
                "terms": {{
                  "connectionstatus.keyword": [
                    "ACTIVE"
                  ]
                }}
              }},
             {{
                "range": {{
                  "createddate": {{
                  "gte": {0},
                  "lte": {1},
                  "format": "epoch_millis"
                }}
                }}
              }}
            ]
          }}
        }},
            "aggs": {{
                "ward": {{
                  "terms": {{
                    "field": "block.keyword"
                  }},
              "aggs": {{
                "ulb": {{
                  "terms": {{
                    "field": "cityname.keyword"
                  }},
                "aggs": {{
                  "region": {{
                    "terms": {{
                      "field": "regionname.keyword"
                    }},
                    "aggs": {{
                      "channelType": {{
                        "terms": {{
                          "field": "channel.keyword"
                        }}
                    }}
    }}
    }}
    }}
    }}
    }}
    }}
    }}
    }}


"""
}


def extract_ws_total_transactions(metrics, region_bucket):
    status_agg = region_bucket.get('applicationsMovedToday')
    status_buckets = status_agg.get('buckets')
    grouped_by = []
    for status_bucket in status_buckets:
        grouped_by.append({'name': status_bucket.get('key'), 'value': status_bucket.get(
            'applicationsMovedToday').get('value') if status_bucket.get('applicationsMovedToday') else 0})
    metrics['applicationsMovedToday'] = [
        {'groupBy': 'status', 'buckets': grouped_by}]
    return metrics


ws_total_transactions = {'path': 'dss-collection_v2/_search',
                         'name': 'ws_total_transactions',
                         'lambda': extract_ws_total_transactions,
                         'query': """
  {{
    "size":0,
    "query": {{
          "bool": {{
            "must_not": [
              {{
                "term": {{
                  "dataObject.tenantId.keyword": "pb.testing"
                }}
              }},
              {{
                "terms": {{
                  "dataObject.paymentDetails.bill.status.keyword": [
                    "Cancelled"
                  ]
                }}
              }}
            ],
            "must": [
              {{
                "terms": {{
                  "dataObject.paymentDetails.businessService.keyword": [
                    "WS",
                    "WS.ONE_TIME_FEE",
                    "SW.ONE_TIME_FEE",
                    "SW"
                  ]
                }}
                }},
             {{
                   "range":{{
                    "dataObject.paymentDetails.receiptDate": {{
                    "gte": {0},
                    "lte": {1},
                    "format": "epoch_millis"
                   }}
                    }}
                    }}
                
             ]
              }}
          }},
  "aggs": {{
        "ward": {{
          "terms": {{
            "field": "domainObject.ward.name.keyword"
          }},
                "aggs": {{
        "ulb" :{{
          "terms": {{
            "field": "domainObject.tenantId.keyword"
          }},
      "aggs": {{
         "region": {{
            "terms": {{
              "field": "dataObject.tenantData.city.districtName.keyword"
            }},
            "aggs":{{
             "transactions": {{
                "value_count": {{
                  "field": "dataObject.transactionNumber.keyword"
            }}
          }}
              
            }}
            
      }}
   }}
}}
}}
}}
}}

  }}


"""
}


ws_queries = [ws_collection_by_payment_channel_type, 
              ws_pending_connections, ws_sewerage_connections, ws_water_connections, ws_todays_applications, ws_closed_applications,  ws_connections_created_by_channel_type]

#the default payload for WS
def empty_ws_payload(region, ulb, ward, date):
    return {
        "date": date,
        "module": "WS",
        "ward": ward,
        "ulb": ulb,
        "region": region,
        "state": "Punjab",
        "metrics": {
            "transactions": 0,
            "connectionsCreated": [
                
            ],
            "todaysCollection": [
               
            ],
            "sewerageConnections": [
              
            ],
            "waterConnections": [
               
            ],
            "pendingConnections": [
              
            ],
             "slaCompliance": 0,
             "todaysTotalApplications": 0, 
             "todaysClosedApplications": 0, 
             "todaysCompletedApplicationsWithinSLA": 0 
        }
    }
