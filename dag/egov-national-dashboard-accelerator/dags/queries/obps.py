"""OBPS National Dashboard extraction queries.

Building plans: bpa-buildingplan-index (mapping supplied).
Collections: dss-collection_v2 (requested name; supplied mapping is for
             egov-dss-ingest-enriched, so verify _mapping before deployment).

IMPORTANT: Empty/zero metrics can be unmeasured rather than actual zero.
Review the accompanying report for unsupported metrics and business assumptions.
"""
import copy

BUILDING_INDEX = 'bpa-buildingplan-index'
PAYMENT_INDEX = 'dss-collection_v2'


def _value(bucket, name, subfield='count'):
    return ((bucket.get(name) or {}).get(subfield) or {}).get('value') or 0


def extract_obps_building(metrics, bucket):
    for key in ('ocIssued','ocSubmitted','applicationsSubmitted',
                'todaysClosedApplicationsOC','todaysClosedApplicationsPermit',
                'todaysApprovedApplications'):
        metrics[key] = int(_value(bucket,key))
    for key in ('averageDaysToIssuePermit','averageDaysToIssueOC'):
        metrics[key] = _value(bucket,key,'days')
    issued = bucket.get('permitsIssued') or {}
    metrics['permitsIssued'] = [
        {'groupBy':group,'buckets':[
            {'name':entry['key'],'value':entry['doc_count']}
            for entry in (issued.get(aggregation) or {}).get('buckets',[])
        ]}
        for group,aggregation in [('riskType','byRiskType'),
                                  ('occupancyType','byOccupancyType'),
                                  ('subOccupancyType','bySubOccupancyType')]
    ]
    # Suboccupancy is not calculated: no confirmed source field/derivation.
    return metrics


def extract_obps_payment(metrics, bucket):
    values={'Digital':0.0,'Non Digital':0.0}
    for item in (bucket.get('paymentMode') or {}).get('buckets',[]):
        mode=str(item.get('key','')).strip().lower()
        amount=float((item.get('amount') or {}).get('value') or 0)
        if mode in ('online','digital','netbanking','net banking','upi','card',
                    'credit card','debit card','internet banking'):
            values['Digital']+=amount
        else:
            # Verify paymentMode vocabulary to classify any other mode.
            values['Non Digital']+=amount
    metrics['todaysCollection']=[{'groupBy':'paymentMode','buckets':[
        {'name':key,'value':value} for key,value in values.items()]}]
    return metrics

obps_building = {
    'path': BUILDING_INDEX + '/_search',
    'name': 'obps_building',
    'lambda': extract_obps_building,
    'query': '''{{
  "size": 0,
  "query": {{
    "bool": {{
      "filter": [
        {{
          "terms": {{
            "Data.businessService.keyword": [
              "BPA",
              "BPA_LOW",
              "BPA_OC"
            ]
          }}
        }},
        {{
          "bool": {{
            "should": [
              {{
                "range": {{
                  "Data.applicationDate": {{
                    "gte": {0},
                    "lte": {1}
                  }}
                }}
              }},
              {{
                "range": {{
                  "Data.approvalDate": {{
                    "gte": {0},
                    "lte": {1}
                  }}
                }}
              }}
            ],
            "minimum_should_match": 1
          }}
        }}
      ],
      "must_not": [
        {{
          "term": {{
            "Data.tenantId.keyword": "pb.testing"
          }}
        }}
      ]
    }}
  }},
  "aggs": {{
    "ward": {{
      "terms": {{
        "field": "Data.additionalDetails.wardnumber.keyword",
        "size": 10000,
        "missing": "UNKNOWN"
      }},
      "aggs": {{
        "ulb": {{
          "terms": {{
            "field": "Data.tenantId.keyword",
            "size": 10000,
            "missing": "UNKNOWN"
          }},
          "aggs": {{
            "region": {{
              "terms": {{
                "field": "Data.additionalDetails.landInfo.address.locality.name.keyword",
                "size": 10000,
                "missing": "UNKNOWN"
              }},
              "aggs": {{
                "ocIssued": {{
                  "filter": {{
                    "bool": {{
                      "filter": [
                        {{
                          "terms": {{
                            "Data.businessService.keyword": [
                              "BPA_OC"
                            ]
                          }}
                        }},
                        {{
                          "term": {{
                            "Data.status.keyword": "APPROVED"
                          }}
                        }},
                        {{
                          "range": {{
                            "Data.approvalDate": {{
                              "gte": {0},
                              "lte": {1}
                            }}
                          }}
                        }}
                      ]
                    }}
                  }},
                  "aggs": {{
                    "count": {{
                      "value_count": {{
                        "field": "Data.applicationNo.keyword"
                      }}
                    }}
                  }}
                }},
                "ocSubmitted": {{
                  "filter": {{
                    "bool": {{
                      "filter": [
                        {{
                          "terms": {{
                            "Data.businessService.keyword": [
                              "BPA_OC"
                            ]
                          }}
                        }},
                        {{
                          "range": {{
                            "Data.applicationDate": {{
                              "gte": {0},
                              "lte": {1}
                            }}
                          }}
                        }}
                      ]
                    }}
                  }},
                  "aggs": {{
                    "count": {{
                      "value_count": {{
                        "field": "Data.applicationNo.keyword"
                      }}
                    }}
                  }}
                }},
                "applicationsSubmitted": {{
                  "filter": {{
                    "bool": {{
                      "filter": [
                        {{
                          "terms": {{
                            "Data.businessService.keyword": [
                              "BPA",
                              "BPA_LOW"
                            ]
                          }}
                        }},
                        {{
                          "range": {{
                            "Data.applicationDate": {{
                              "gte": {0},
                              "lte": {1}
                            }}
                          }}
                        }}
                      ]
                    }}
                  }},
                  "aggs": {{
                    "count": {{
                      "value_count": {{
                        "field": "Data.applicationNo.keyword"
                      }}
                    }}
                  }}
                }},
                "todaysClosedApplicationsOC": {{
                  "filter": {{
                    "bool": {{
                      "filter": [
                        {{
                          "terms": {{
                            "Data.businessService.keyword": [
                              "BPA_OC"
                            ]
                          }}
                        }},
                        {{
                          "term": {{
                            "Data.status.keyword": "APPROVED"
                          }}
                        }},
                        {{
                          "range": {{
                            "Data.approvalDate": {{
                              "gte": {0},
                              "lte": {1}
                            }}
                          }}
                        }}
                      ]
                    }}
                  }},
                  "aggs": {{
                    "count": {{
                      "value_count": {{
                        "field": "Data.applicationNo.keyword"
                      }}
                    }}
                  }}
                }},
                "todaysClosedApplicationsPermit": {{
                  "filter": {{
                    "bool": {{
                      "filter": [
                        {{
                          "terms": {{
                            "Data.businessService.keyword": [
                              "BPA",
                              "BPA_LOW"
                            ]
                          }}
                        }},
                        {{
                          "term": {{
                            "Data.status.keyword": "APPROVED"
                          }}
                        }},
                        {{
                          "range": {{
                            "Data.approvalDate": {{
                              "gte": {0},
                              "lte": {1}
                            }}
                          }}
                        }}
                      ]
                    }}
                  }},
                  "aggs": {{
                    "count": {{
                      "value_count": {{
                        "field": "Data.applicationNo.keyword"
                      }}
                    }}
                  }}
                }},
                "averageDaysToIssuePermit": {{
                  "filter": {{
                    "bool": {{
                      "filter": [
                        {{
                          "terms": {{
                            "Data.businessService.keyword": [
                              "BPA",
                              "BPA_LOW"
                            ]
                          }}
                        }},
                        {{
                          "term": {{
                            "Data.status.keyword": "APPROVED"
                          }}
                        }},
                        {{
                          "range": {{
                            "Data.approvalDate": {{
                              "gte": {0},
                              "lte": {1}
                            }}
                          }}
                        }}
                      ]
                    }}
                  }},
                  "aggs": {{
                    "days": {{
                      "avg": {{
                        "script": {{
                          "lang": "painless",
                          "source": "if (doc['Data.approvalDate'].size()==0 || doc['Data.applicationDate'].size()==0) return null; long delta=doc['Data.approvalDate'].value-doc['Data.applicationDate'].value; return delta>=0 ? delta/86400000.0 : null;"
                        }}
                      }}
                    }}
                  }}
                }},
                "averageDaysToIssueOC": {{
                  "filter": {{
                    "bool": {{
                      "filter": [
                        {{
                          "terms": {{
                            "Data.businessService.keyword": [
                              "BPA_OC"
                            ]
                          }}
                        }},
                        {{
                          "term": {{
                            "Data.status.keyword": "APPROVED"
                          }}
                        }},
                        {{
                          "range": {{
                            "Data.approvalDate": {{
                              "gte": {0},
                              "lte": {1}
                            }}
                          }}
                        }}
                      ]
                    }}
                  }},
                  "aggs": {{
                    "days": {{
                      "avg": {{
                        "script": {{
                          "lang": "painless",
                          "source": "if (doc['Data.approvalDate'].size()==0 || doc['Data.applicationDate'].size()==0) return null; long delta=doc['Data.approvalDate'].value-doc['Data.applicationDate'].value; return delta>=0 ? delta/86400000.0 : null;"
                        }}
                      }}
                    }}
                  }}
                }},
                "permitsIssued": {{
                  "filter": {{
                    "bool": {{
                      "filter": [
                        {{
                          "terms": {{
                            "Data.businessService.keyword": [
                              "BPA",
                              "BPA_LOW"
                            ]
                          }}
                        }},
                        {{
                          "term": {{
                            "Data.status.keyword": "APPROVED"
                          }}
                        }},
                        {{
                          "range": {{
                            "Data.approvalDate": {{
                              "gte": {0},
                              "lte": {1}
                            }}
                          }}
                        }}
                      ]
                    }}
                  }},
                  "aggs": {{
                    "byRiskType": {{
                      "terms": {{
                        "field": "Data.additionalDetails.riskType.keyword",
                        "size": 100
                      }}
                    }},
                    "byOccupancyType": {{
                      "terms": {{
                        "field": "Data.additionalDetails.landInfo.unit.occupancyType.keyword",
                        "size": 100
                      }}
                    }}
                  }}
                }},
                "todaysApprovedApplications": {{
                  "filter": {{
                    "bool": {{
                      "filter": [
                        {{
                          "terms": {{
                            "Data.businessService.keyword": [
                              "BPA",
                              "BPA_LOW"
                            ]
                          }}
                        }},
                        {{
                          "term": {{
                            "Data.status.keyword": "APPROVED"
                          }}
                        }},
                        {{
                          "range": {{
                            "Data.approvalDate": {{
                              "gte": {0},
                              "lte": {1}
                            }}
                          }}
                        }}
                      ]
                    }}
                  }},
                  "aggs": {{
                    "count": {{
                      "value_count": {{
                        "field": "Data.applicationNo.keyword"
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
}}'''
}

obps_collection = {
    'path': PAYMENT_INDEX + '/_search',
    'name': 'obps_collection',
    'lambda': extract_obps_payment,
    'query': '''{{
  "size": 0,
  "query": {{
    "bool": {{
      "filter": [
        {{
          "terms": {{
            "dataObject.paymentDetails.businessService.keyword": [
              "BPA.LOW_RISK_PERMIT_FEE",
              "BPA.NC_APP_FEE",
              "BPA.NC_SAN_FEE",
              "BPA.NC_OC_APP_FEE"
            ]
          }}
        }},
        {{
          "range": {{
            "dataObject.paymentDetails.receiptDate": {{
              "gte": {0},
              "lte": {1}
            }}
          }}
        }}
      ],
      "must_not": [
        {{
          "term": {{
            "dataObject.tenantId.keyword": "pb.testing"
          }}
        }},
        {{
          "term": {{
            "dataObject.paymentStatus.keyword": "Cancelled"
          }}
        }}
      ]
    }}
  }},
  "aggs": {{
    "ward": {{
      "terms": {{
        "field": "dataObject.ward.name.keyword",
        "size": 10000,
        "missing": "UNKNOWN"
      }},
      "aggs": {{
        "ulb": {{
          "terms": {{
            "field": "dataObject.tenantId.keyword",
            "size": 10000,
            "missing": "UNKNOWN"
          }},
          "aggs": {{
            "region": {{
              "terms": {{
                "field": "dataObject.tenantData.city.districtName.keyword",
                "size": 10000,
                "missing": "UNKNOWN"
              }},
              "aggs": {{
                "paymentMode": {{
                  "terms": {{
                    "field": "dataObject.paymentMode.keyword",
                    "size": 100
                  }},
                  "aggs": {{
                    "amount": {{
                      "sum": {{
                        "field": "dataObject.paymentDetails.totalAmountPaid"
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
}}'''
}

_DEFAULT_METRICS = {'ocPlansScrutinized': 0, 'plansScrutinized': 0, 'ocSubmitted': 0, 'applicationsSubmitted': 0, 'ocIssued': 0, 'landAreaAppliedInSystemForBPA': 0, 'averageDaysToIssuePermit': 0, 'averageDaysToIssueOC': 0, 'todaysClosedApplicationsOC': 0, 'todaysCompletedApplicationsWithinSLAOC': 0, 'todaysClosedApplicationsPermit': 0, 'todaysCompletedApplicationsWithinSLAPermit': 0, 'slaComplianceOC': 0, 'slaCompliancePermit': 0, 'applicationsWithDeviation': 0, 'averageDeviation': 0, 'ocWithDeviation': 0, 'todaysCollection': [{'groupBy': 'paymentMode', 'buckets': [{'name': 'Digital', 'value': 0}, {'name': 'Non Digital', 'value': 0}]}], 'permitsIssued': [{'groupBy': 'riskType', 'buckets': []}, {'groupBy': 'occupancyType', 'buckets': []}, {'groupBy': 'subOccupancyType', 'buckets': []}], 'pendingApplicationsBeyondTimeline': 0, 'todaysApprovedApplications': 0, 'todaysApprovedApplicationsWithinSLA': 0, 'avgDaysForApplicationApproval': 0, 'StipulatedDays': 0}

obps_queries = [obps_building, obps_collection]

# Unmapped: ocPlansScrutinized, plansScrutinized, landAreaAppliedInSystemForBPA,
# SLA metrics, deviations, pending timelines, subOccupancyType and StipulatedDays.
# Their default values remain zero as placeholders; do NOT interpret as measured.
def empty_obps_payload(region, ulb, ward, date):
    return {'date':date,'module':'OBPS','ward':ward,'ulb':ulb,
            'region':region,'state':'Punjab','metrics':copy.deepcopy(_DEFAULT_METRICS)}
