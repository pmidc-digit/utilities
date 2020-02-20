import requests
import sys
import json

# The first argument to the script is the job name
argumentList = sys.argv 
job_name = sys.argv[1]


# Calls MDMS service to fetch cron job API endpoints configuration

mdms_url = "http://egov-mdms-service.egov:8080/egov-mdms-service/v1/_search"
mdms_payload = "{\n \"RequestInfo\": {\n   \"apiId\": \"asset-services\",\n   \"ver\": null,\n   \"ts\": null,\n   \"action\": null,\n   \"did\": null,\n   \"key\": null,\n   \"msgId\": \"search with from and to values\",\n   \"authToken\": \"f81648a6-bfa0-4a5e-afc2-57d751f256b7\"\n },\n \"MdmsCriteria\": {\n   \"tenantId\": \"pb\",\n   \"moduleDetails\": [\n     {\n       \"moduleName\": \"common-masters\",\n       \"masterDetails\": [\n         {\n           \"name\": \"CronJobAPIConfig\"\n         }\n       ]\n     }\n   ]\n }\n}"
mdms_headers = {
  'Content-Type': 'application/json'
}
response = requests.request("POST", mdms_url, headers=mdms_headers, data = mdms_payload)

# Convert the response to json
mdms_data = response.json()


# Call user search to fetch SYSTEM user
user_url = "http://egov-user.egov:8080/user/v1/_search?tenantId=pb"
user_payload = "{\n\t\"requestInfo\" :{\n   \"apiId\": \"ap.public\",\n    \"ver\": \"1\",\n    \"ts\": 45646456,\n    \"action\": \"POST\",\n    \"did\": null,\n    \"key\": null,\n    \"msgId\": \"8c11c5ca-03bd-11e7-93ae-92361f002671\",\n    \"userInfo\": {\n    \t\"id\" : 32\n    },\n    \"authToken\": \"4f7e2e9c-e6e8-4320-8f40-3d4d6ffb7505\"\n\t},\n\t\n   \"tenantId\" : \"pb\",\n   \"userType\":\"SYSTEM\",\n   \"userName\" : \"CRONJOB\",\n   \"pageSize\": \"1\",\n   \"roleCodes\" : [\"ANONYMOUS\"]\n\n\n}\n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n   \n}\n"
user_headers = {
  'Content-Type': 'application/json'
}
user_response = requests.request("POST", user_url, headers=user_headers, data = user_payload)
users = user_response.json()['user']
if len(users)==0:
    raise Exception("System user not found")
else:
    userInfo = users[0]



RequestInfo = "{\n  \"RequestInfo\": {\n    \"apiId\": \"Rainmaker\",\n    \"ver\": \".01\",\n    \"action\": \"\",\n    \"did\": \"1\",\n    \"key\": \"\",\n    \"msgId\": \"20170310130900|en_IN\",\n    \"requesterId\": \"\",\n    \"userInfo\": \"{SYSTEM_USER}\"\n  }\n}"
RequestInfo = RequestInfo.replace("{SYSTEM_USER}",str(userInfo))



# Looping through each entry in the config, it checks if the active flag is true and jobName 
# matches the job name given as argument if both criteria fulfilled the given http request is called

for data in mdms_data["MdmsRes"]["common-masters"]["CronJobs"]:
    
    params = None
    payload = None
    headers = None

    if data['active'] and data['jobName']==job_name:
        method = data['method']
        url = data['url']
        
        if 'header' in data.keys():
            headers = data['header']
            
        if 'payload' in data.keys():    
            payload = data['payload']
            if 'RequestInfo' in data['payload']:
                if data['payload']['RequestInfo']=='{DEFAULT_REQUESTINFO}':
                    data['payload']['RequestInfo'] = RequestInfo
            
        if 'parmas' in data.keys():
            params = data['params']
            
        res = requests.request(method, url, params = json.dumps(params), headers = headers, data = json.dumps(payload))