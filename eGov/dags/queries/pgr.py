
def extract_pgr_closed_complaints(metrics, region_bucket):
    metrics['closedComplaints'] = region_bucket.get('closedComplaints').get(
        'value') if region_bucket.get('closedComplaints') else 0
    return metrics


pgr_closed_complaints = {
  'path': 'pgrindex-v1-enriched/_search',
    'name': 'pgr_closed_complaints',
    'lambda': extract_pgr_closed_complaints,
    'query': """
   {{
  "size": 0,
    "query": {{
        "bool": {{
          "must_not": [
            {{
              "term": {{
                "Data.tenantId.keyword": "pb.testing"
              }}
            }}
          ],
          "must": [
            {{
                "range": {{
                    "Data.dateOfComplaint": {{
                    "gte": {0},
                    "lte": {1},
                    "format": "epoch_millis"
                }}
              }}
            }}
            ],
          "filter" :{{
              "terms": {{
                "Data.status.keyword": [
                    "closed",
                    "resolved",
                    "rejected"
                ]
              }}
            }}
        }}
      }},
        "aggs": {{
        "ward": {{
          "terms": {{
            "field": "Data.complaintWard.name.keyword"
          }},
           "aggs": {{
            "ulb": {{
            "terms": {{
              "field": "Data.tenantId.keyword"
            }},
            "aggs": {{
              "region": {{
                "terms": {{
                  "field": "Data.tenantData.city.districtName.keyword"
                }},
                       "aggs": {{
                          	"closedComplaints": {{
                            "value_count": {{
                              "field": "Data.dateOfComplaint"
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


def extract_pgr_resolved_complaints(metrics, region_bucket):
    metrics['resolvedComplaints'] = region_bucket.get('resolvedComplaints').get(
        'value') if region_bucket.get('resolvedComplaints') else 0
    return metrics


pgr_resolved_complaints = {
    'path': 'pgrindex-v1-enriched/_search',
    'name': 'pgr_resolved_complaints',
    'lambda': extract_pgr_resolved_complaints,
    'query': """
  {{
  "size": 0,
    "query": {{
        "bool": {{
          "must_not": [
            {{
              "term": {{
                "Data.tenantId.keyword": "pb.testing"
              }}
            }}
          ],
          "must":[
            {{
               "range": {{
                    "Data.dateOfComplaint": {{
                    "gte": {0},
                    "lte": {1},
                    "format": "epoch_millis"
                }}
              }}
            }}],
          "filter" :{{
              "terms": {{
                "Data.status.keyword": [
                  "resolved"
                ]
              }}
            }}
          }}
      }},
      "aggs": {{
        "ward": {{
          "terms": {{
            "field": "Data.complaintWard.name.keyword"
          }},
       "aggs": {{
            "ulb": {{
              "terms": {{
                "field": "Data.tenantId.keyword"
              }},
        "aggs": {{
          "region": {{
            "terms": {{
              "field": "Data.tenantData.city.districtName.keyword"
            }},
                "aggs": {{
                  "resolvedComplaints": {{
                        "value_count": {{
                          "field": "Data.dateOfComplaint"
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


def extract_pgr_unique_citizens(metrics, region_bucket):
 # metrics['todaysLicenseIssued'] = region_bucket.get('todaysLicenseIssued').get('value') if region_bucket.get('todaysLicenseIssued') else 0
    metrics['uniqueCitizens'] = region_bucket.get('uniqueCitizens').get(
        'value') if region_bucket.get('uniqueCitizens') and region_bucket.get('uniqueCitizens').get('value') else 0
    return metrics


pgr_unique_citizens = {
    'path': 'pgrindex-v1-enriched/_search',

    'name': 'pgr_unique_citizens',
    'lambda': extract_pgr_unique_citizens,
    'query': """
    {{
    "size": 0,
      "query": {{
          "bool": {{
            "must_not": [
              {{
                "term": {{
                  "Data.tenantId.keyword": "pb.testing"
                }}
              }}
            ],
        "must":[
            {{
               "range": {{
                    "Data.dateOfComplaint": {{
                    "gte": {0},
                    "lte": {1},
                    "format": "epoch_millis"
                }}
              }}
            }}]
          }}
        }},
        "aggs": {{
              "ward": {{
                "terms": {{
                  "field": "Data.complaintWard.name.keyword"
                }},
            "aggs": {{
              "ulb": {{
                "terms": {{
                  "field": "Data.tenantId.keyword"
                }},
            "aggs": {{
              "region": {{
                "terms": {{
                  "field": "Data.tenantData.city.districtName.keyword"
                }},
            "aggs": {{
              "uniqueCitizens": {{
                "cardinality": {{
                  "field": "Data.citizen.uuid.keyword"
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


def extract_pgr_sla_achieved(metrics, region_bucket):
 # metrics['todaysLicenseIssued'] = region_bucket.get('todaysLicenseIssued').get('value') if region_bucket.get('todaysLicenseIssued') else 0
    sla_agg = region_bucket.get('slaAchievement')
    sla_achievement = 0
    if sla_agg:
      sla_buckets = sla_agg.get('buckets')
      for sla_bucket in sla_buckets:
         sla_achievement = sla_bucket.get('slaAchievement').get(
                              'value') if sla_bucket.get('slaAchievement')  else 0
    metrics['slaAchievement'] = sla_achievement
    
    return metrics


pgr_sla_achieved = {
    'path': 'pgrindex-v1-enriched/_search',

    'name': 'pgr_sla_achieved',
    'lambda': extract_pgr_sla_achieved,
    'query': """
 {{
  "size": 0,
    "query": {{
        "bool": {{
          "must_not": [
            {{
              "term": {{
                "Data.tenantId.keyword": "pb.testing"
              }}
            }}
          ],
      "must":[
            {{
               "range": {{
                    "Data.dateOfComplaint": {{
                    "gte": {0},
                    "lte": {1},
                    "format": "epoch_millis"
                }}
              }}
            }}]
        }}
      }},
       "aggs": {{
            "ward": {{
              "terms": {{
                "field": "Data.complaintWard.name.keyword"
              }},
           "aggs": {{
            "ulb": {{
              "terms": {{
                "field": "Data.tenantId.keyword"
              }},
              "aggs": {{
                "region": {{
                  "terms": {{
                    "field": "Data.tenantData.city.districtName.keyword"
                  }},
              "aggs":{{
                  "slaAchievement": {{
                    "range": {{
                      "field": "Data.slaHours", 
                      "ranges": [
                       {{ "from": 0 ,"to": 360}}
                      
                      ]
                    }},
                    "aggs": {{
                        "slaAchievement": {{
                          "value_count": {{ "field": "Data.slaHours" }}
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


def extract_pgr_completion_rate(metrics, region_bucket):
  completion_rate_value = 0
  completion_rate = region_bucket.get('completionRate')

  if completion_rate:
    completion_rate_buckets = completion_rate.get('buckets')
    if completion_rate_buckets and completion_rate_buckets.get('all'):
      all_bucket = completion_rate_buckets.get('all')
      if all_bucket:
        completion_rate_value = all_bucket.get('completionRate').get('value') if all_bucket.get('completionRate') else 0

  metrics['completionRate'] = completion_rate_value
  return metrics


pgr_completion_rate = {
    'path': 'pgrindex-v1-enriched/_search',

    'name': 'pgr_completion_rate',
    'lambda': extract_pgr_completion_rate,
    'query': """
 {{
  "size": 0,
   "query": {{
        "bool": {{
          "must_not": [
            {{
              "term": {{
                "Data.tenantId.keyword": "pb.testing"
              }}
            }}
          ],
        "must":[
            {{
               "range": {{
                    "Data.dateOfComplaint": {{
                    "gte": {0},
                    "lte": {1},
                    "format": "epoch_millis"
                }}
              }}
            }}]
        }}
      }},
      "aggs": {{
        "ward": {{
          "terms": {{
            "field": "Data.complaintWard.name.keyword"
          }},
       "aggs": {{
            "ulb": {{
              "terms": {{
                "field": "Data.tenantId.keyword"
              }},
        "aggs": {{
          "region": {{
            "terms": {{
              "field": "Data.tenantData.city.districtName.keyword"
            }},
  "aggs": {{
    "completionRate": {{
      "filters": {{
        "filters": {{
          "all": {{
            "match_all": {{}}
          }}
        }}
      }},
    "aggs": {{
        "totalComplaints": {{
          "value_count": {{
             "field": "Data.dateOfComplaint"
          }}
        }},
    "closedComplaints": {{
      "filter": {{
        "terms": {{
          "Data.status.keyword": [
            "closed",
            "resolved",
            "rejected"
          ]
        }}
      }},
       "aggs": {{
            "complaints": {{
              "value_count": {{
                "field": "Data.dateOfComplaint"
              }}
            }}
          }}
        }},
        "completionRate": {{
          "bucket_script": {{
            "buckets_path": {{
              "closed": "closedComplaints>complaints",
              "total": "totalComplaints"
            }},
            "script": "params.closed / params.total * 100"
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


def extract_pgr_todays_complaints(metrics, region_bucket):
    status_agg = region_bucket.get('Complaints By Status')
    status_buckets = status_agg.get('buckets')
    allDims = []
    grouped_by = []
    for status_bucket in status_buckets:
        grouped_by.append({'name': status_bucket.get('key'), 'value': status_bucket.get(
            'byStatus').get('value') if status_bucket.get('byStatus') else 0})
    allDims.append(
        {'groupBy': 'status', 'buckets': grouped_by})

    channel_agg = region_bucket.get('Complaints By Channels')
    channel_buckets = channel_agg.get('buckets')
    grouped_by = []
    for channel_bucket in channel_buckets:
        grouped_by.append({'name': channel_bucket.get('key'), 'value': channel_bucket.get(
            'byChannel').get('value') if channel_bucket.get('byChannel') else 0})
    allDims.append(
        {'groupBy': 'channel', 'buckets': grouped_by})

    department_agg = region_bucket.get('Complaints By Department')
    department_buckets = department_agg.get('buckets')
    grouped_by = []
    for department_bucket in department_buckets:
        grouped_by.append({'name': department_bucket.get('key'), 'value': department_bucket.get(
            'byDepartment').get('value') if department_bucket.get('byDepartment') else 0})
    allDims.append(
        {'groupBy': 'department', 'buckets': grouped_by})

    category_agg = region_bucket.get('Complaints By Category')
    category_buckets = category_agg.get('buckets')
    grouped_by = []
    for category_bucket in category_buckets:
        grouped_by.append({'name': category_bucket.get('key'), 'value': category_bucket.get(
            'byCategory').get('value') if category_bucket.get('byCategory') else 0})
    allDims.append(
        {'groupBy': 'category', 'buckets': grouped_by})


    metrics['todaysComplaints'] = allDims
    return metrics

pgr_todays_complaints = {
    'path': 'pgrindex-v1-enriched/_search',

    'name': 'pgr_todays_complaints',
    'lambda': extract_pgr_todays_complaints,
    'query': """
  {{
  "size": 0,
    "query": {{
        "bool": {{
          "must_not": [
            {{
              "term": {{
                "Data.tenantId.keyword": "pb.testing"
              }}
            }}
          ],
        "must":[
            {{
               "range": {{
                    "Data.dateOfComplaint": {{
                    "gte": {0},
                    "lte": {1},
                    "format": "epoch_millis"
                }}
              }}
            }}]
        }}
      }},
       "aggs": {{
            "ward": {{
              "terms": {{
                "field": "Data.complaintWard.name.keyword"
              }},
           "aggs": {{
            "ulb": {{
              "terms": {{
                "field": "Data.tenantId.keyword"
              }},
              "aggs": {{
                "region": {{
                  "terms": {{
                    "field": "Data.tenantData.city.districtName.keyword"
                  }},
                    "aggs": {{
                    "Complaints By Status": {{
                        "terms": {{
                          "field": "Data.status.keyword"
                        }},
                        "aggs": {{
                          "byStatus": {{
                            "value_count": {{
                              "field": "Data.dateOfComplaint"
                            }}
                          }}
                        }}
                      }},
                  "Complaints By Channels": {{
                      "terms": {{
                        "field": "Data.source.keyword"
                      }},
                      "aggs": {{
                        "byChannel": {{
                          "value_count": {{
                            "field": "Data.dateOfComplaint"
                          }}
                        }}
                      }}
                    }},
                  "Complaints By Department": {{
                  "terms": {{
                    "field": "Data.department.keyword"
                  }},
                    "aggs": {{
                      "byDept": {{
                        "value_count": {{
                          "field": "Data.dateOfComplaint"
                        }}
                      }}
                    }}
                   }},
                 "Complaints By Category": {{
                    "terms": {{
                              "field": "Data.complainCategory.keyword"
                          }},
                    "aggs": {{
                      "byCategory": {{
                        "value_count": {{
                          "field": "Data.dateOfComplaint"
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


def extract_pgr_status(metrics, region_bucket):
    department_agg = region_bucket.get('Department')
    department_buckets = department_agg.get('buckets')
    todaysReopenedComplaints = { 'groupBy' : 'department', 'buckets' : []}
    todaysOpenComplaints = { 'groupBy' : 'department', 'buckets' : []}
    todaysAssignedComplaints = { 'groupBy' : 'department', 'buckets' : []}
    todaysRejectedComplaints = { 'groupBy' : 'department', 'buckets' : []}
    todaysReassignedComplaints = { 'groupBy' : 'department', 'buckets' : []}

    for department_bucket in department_buckets:
      department_name = department_bucket.get('key')
      todaysReopenedComplaints['buckets'].append( { 'name' : department_name, 'value' : department_bucket.get('todaysReopenedComplaints').get('todaysReopenedComplaints').get('value') if department_bucket.get('todaysReopenedComplaints') else 0})
      todaysOpenComplaints['buckets'].append( { 'name' : department_name, 'value' : department_bucket.get('todaysOpenComplaints').get('todaysOpenComplaints').get('value') if department_bucket.get('todaysOpenComplaints') else 0})
      todaysAssignedComplaints['buckets'].append( { 'name' : department_name, 'value' : department_bucket.get('todaysAssignedComplaints').get('todaysAssignedComplaints').get('value') if department_bucket.get('todaysAssignedComplaints') else 0})
      todaysRejectedComplaints['buckets'].append( { 'name' : department_name, 'value' : department_bucket.get('todaysRejectedComplaints').get('todaysRejectedComplaints').get('value') if department_bucket.get('todaysRejectedComplaints') else 0})
      todaysReassignedComplaints['buckets'].append( { 'name' : department_name, 'value' : department_bucket.get('todaysReassignedComplaints').get('todaysReassignedComplaints').get('value') if department_bucket.get('todaysReassignedComplaints') else 0})


    metrics['todaysReopenedComplaints'] = [todaysReopenedComplaints]
    metrics['todaysOpenComplaints'] = [todaysOpenComplaints]
    metrics['todaysAssignedComplaints'] = [todaysAssignedComplaints]
    metrics['todaysRejectedComplaints'] = [todaysRejectedComplaints]
    metrics['todaysReassignedComplaints'] = [todaysReassignedComplaints]
    

    return metrics


pgr_status = {
  
  'path': 'pgrindex-v1-enriched/_search',

    'name': 'pgr_status',
    'lambda': extract_pgr_status,
    'query': """
  {{
  "size": 0,
    "query": {{
        "bool": {{
          "must_not": [
            {{
              "term": {{
                "Data.tenantId.keyword": "pb.testing"
              }}
            }}
          ],
        "must":[
            {{
               "range": {{
                    "Data.dateOfComplaint": {{
                    "gte": {0},
                    "lte": {1},
                    "format": "epoch_millis"
                }}
              }}
            }}]
        }}
    }},
       "aggs": {{
            "ward": {{
              "terms": {{
                "field": "Data.complaintWard.name.keyword"
              }},
           "aggs": {{
            "ulb": {{
              "terms": {{
                "field": "Data.tenantId.keyword"
              }},
              "aggs": {{
                "region": {{
                  "terms": {{
                    "field": "Data.tenantData.city.districtName.keyword"
                  }},
                    "aggs": {{
                        "Department": {{
                          "terms": {{
                            "field": "Data.department.keyword"
                          }},
                          "aggs": {{
                            "todaysRejectedComplaints": {{
                              "filter": {{
                                "terms": {{
                                  "Data.status.keyword": [
                                    "rejected"
                                  ]
                                }}
                              }},
                              "aggs": {{
                                "todaysRejectedComplaints": {{
                                  "value_count": {{
                                    "field": "Data.tenantId.keyword"
                                  }}
                                }}
                              }}
                }},
                            "todaysOpenComplaints": {{
                              "filter": {{
                                "terms": {{
                                  "Data.status.keyword": [
                                    "open"
                                  ]
                                }}
                              }},
                              "aggs": {{
                                "todaysOpenComplaints": {{
                                  "value_count": {{
                                    "field": "Data.tenantId.keyword"
                                  }}
                                }}
                              }}
                }},
                            "todaysAssignedComplaints": {{
                              "filter": {{
                                "terms": {{
                                  "Data.status.keyword": [
                                    "assign",
		                              	"assigned"
                                  ]
                                }}
                              }},
                              "aggs": {{
                                "todaysAssignedComplaints": {{
                                  "value_count": {{
                                    "field": "Data.tenantId.keyword"
                                  }}
                                }}
                              }}
                }},
                            "todaysReopenedComplaints": {{
                              "filter": {{
                                "terms": {{
                                  "Data.actionHistory.actions.action.keyword": [
                                    "reopen"
                                  ]
                                }}
                              }},
                              "aggs": {{
                                "todaysReopenedComplaints": {{
                                  "value_count": {{
                                    "field": "Data.tenantId.keyword"
                                  }}
                                }}
                              }}
                }},
                            "todaysReassignedComplaints": {{
                              "filter": {{
                                "terms": {{
                                  "Data.status.keyword": [
                                    "reassignrequested"  
                                  ]
                                }}
                              }},
                              "aggs": {{
                                "todaysReassignedComplaints": {{
                                  "value_count": {{
                                    "field": "Data.tenantId.keyword"
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
        }}
      }}



    """
}

def extract_pgr_avg_solution_time(metrics, region_bucket):
    department_agg = region_bucket.get('Department')
    department_buckets = department_agg.get('buckets')
    averageSolutionTime = { 'groupBy' : 'department', 'buckets' : []}
  
    for department_bucket in department_buckets:
      department_name = department_bucket.get('key')
      averageSolutionTime['buckets'].append( { 'name' : department_name, 'value' : department_bucket.get('averageSolutionTime').get('value') if department_bucket.get('averageSolutionTime') else 0})


    metrics['averageSolutionTime'] = [averageSolutionTime]
    
    return metrics


pgr_avg_solution_time = {
  
  'path': 'pgrindex-v1-enriched/_search',

    'name': 'pgr_avg_solution_time',
    'lambda': extract_pgr_avg_solution_time,
    'query': """
    {{
    "size":0,
    "query": {{
          "bool": {{
            "must_not": [
              {{
                "term": {{
                  "Data.tenantId.keyword": "pb.testing"
                }}
              }}
            ],
          "must":[
              {{
                 "range": {{
                      "Data.dateOfComplaint": {{
                      "gte": {0},
                      "lte": {1},
                      "format": "epoch_millis"
                  }}
                }}
              }}]
          }}
      }},
    "aggs": {{
          "ward": {{
            "terms": {{
              "field": "Data.complaintWard.name.keyword"
            }},
             "aggs": {{
              "ulb": {{
              "terms": {{
                "field": "Data.tenantId.keyword"
              }},
              "aggs": {{
                "region": {{
                  "terms": {{
                    "field": "Data.tenantData.city.districtName.keyword"
                  }},
                  "aggs":{{
                     "Department": {{
                       "terms": {{
                      "field": "Data.department.keyword"
                       }}, 
                          "aggs": {{
                            "average_time": {{
                            "avg": {{
                              "script": {{
                                "source": "(doc['Data.addressDetail.auditDetails.lastModifiedTime'].value - doc['Data.addressDetail.auditDetails.createdTime'].value)/(3600*1000)"
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
  }}



    """
}

pgr_queries = [pgr_closed_complaints, pgr_resolved_complaints, pgr_unique_citizens, pgr_sla_achieved, pgr_completion_rate, pgr_todays_complaints, pgr_status, pgr_avg_solution_time]
# , pgr_resolved_complaints, pgr_unique_citizens,
# pgr_sla_achieved, pgr_completion_rate, pgr_todays_complaints, pgr_status, pgr_avg_solution_time


def empty_pgr_payload(region, ulb, ward, date):
    return {
        "date": date,
        "module": "PGR",
        "ward": ward,
        "ulb": ulb,
        "region": region,
        "state": "Punjab",
        "metrics":  {
            "closedComplaints": 0,
            "slaAchievement": 0,
                  "completionRate": 0,
                  "uniqueCitizens": 0,
                  "resolvedComplaints": 0,
                  "todaysComplaints": [
                      {
                          "groupBy": "status",
                          "buckets": [
                              {
                                  "name": "reopened",
                                  "value": 20
                              },
                              {
                                  "name": "open",
                                  "value": 23
                              },
                              {
                                  "name": "assigned",
                                  "value": 20
                              },
                              {
                                  "name": "rejected",
                                  "value": 18
                              },
                              {
                                  "name": "reassign",
                                  "value": 12
                              }
                          ]
                      },
                      {
                          "groupBy": "channel",
                          "buckets": [
                              {
                                  "name": "MOBILE",
                                  "value": 40
                              },
                              {
                                  "name": "WEB",
                                  "value": 70
                              }

                          ]
                      },
                      {
                          "groupBy": "department",
                          "buckets": [
                              {
                                  "name": "DEPT1",
                                  "value": 35
                              },
                              {
                                  "name": "DEPT2",
                                  "value": 55
                              },
                              {
                                  "name": "DEPT3",
                                  "value": 40
                              }
                          ]
                      },
                      {
                          "groupBy": "category",
                          "buckets": [
                              {
                                  "name": "Street Lights",
                                  "value": 25
                              },
                              {
                                  "name": "Road Repair",
                                  "value": 45
                              },
                              {
                                  "name": "Garbage Cleaning",
                                  "value": 24
                              },
                              {
                                  "name": "Drainage Issue",
                                  "value": 15
                              }
                          ]
                      }
                  ],
            "todaysReopenedComplaints": [
                      {
                          "groupBy": "department",
                          "buckets": [
                              {
                                  "name": "DEPT1",
                                  "value": 25
                              },
                              {
                                  "name": "DEPT2",
                                  "value": 24
                              },
                              {
                                  "name": "DEPT3",
                                  "value": 25
                              }
                          ]
                      }
                  ],
            "todaysOpenComplaints": [
                      {
                          "groupBy": "department",
                          "buckets": [
                              {
                                  "name": "DEPT1",
                                  "value": 55
                              },
                              {
                                  "name": "DEPT2",
                                  "value": 70
                              },
                              {
                                  "name": "DEPT3",
                                  "value": 110
                              }
                          ]
                      }
                  ],
            "todaysAssignedComplaints": [
                      {
                          "groupBy": "department",
                          "buckets": [
                              {
                                  "name": "DEPT1",
                                  "value": 10
                              },
                              {
                                  "name": "DEPT2",
                                  "value": 5
                              },
                              {
                                  "name": "DEPT3",
                                  "value": 8
                              }
                          ]
                      }
                  ],
            "averageSolutionTime": [
                      {
                          "groupBy": "department",
                          "buckets": [
                              {
                                  "name": "DEPT1",
                                  "value": 5
                              },
                              {
                                  "name": "DEPT2",
                                  "value": 8
                              },
                              {
                                  "name": "DEPT3",
                                  "value": 6
                              }
                          ]
                      }
                  ],
            "todaysRejectedComplaints": [
                      {
                          "groupBy": "department",
                          "buckets": [
                              {
                                  "name": "DEPT1",
                                  "value": 7
                              },
                              {
                                  "name": "DEPT2",
                                  "value": 10
                              },
                              {
                                  "name": "DEPT3",
                                  "value": 0
                              }
                          ]
                      }
                  ],
            "todaysReassignedComplaints": [
                      {
                          "groupBy": "department",
                          "buckets": [
                              {
                                  "name": "DEPT1",
                                  "value": 8
                              },
                              {
                                  "name": "DEPT2",
                                  "value": 5
                              },
                              {
                                  "name": "DEPT3",
                                  "value": 9
                              }
                          ]
                      }
                  ]
        }
    }
