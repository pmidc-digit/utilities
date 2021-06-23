package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"strings"
	"text/template"
)

type Limiter struct {
	URL             string
	LimitingType    string
	Rate            float64
	Quota           float64
	RefreshInterval float64
	Service         string
}

const limiterTemplate string = `
{{- range . }}  
zuul.ratelimit.policy-list.{{ .Service }}[0].limit={{ .Rate }}
zuul.ratelimit.policy-list.{{ .Service }}[0].quota={{ .Quota }}
zuul.ratelimit.policy-list.{{ .Service }}[0].refresh-interval={{ .RefreshInterval }}
zuul.ratelimit.policy-list.{{ .Service }}[0].type[0]=url={{ .URL }}
zuul.ratelimit.policy-list.{{ .Service }}[0].type[1]={{ .LimitingType }}
{{ end -}}
`
const fixedProperties string = `
zuul.ratelimit.enabled=true
zuul.ratelimit.repository=REDIS
zuul.ratelimit.behind-proxy=true
zuul.ratelimit.add-response-headers=true`

func getMDMSData() {

	//host, _ := os.LookupEnv("HOST")
	//tenantId, _ := os.LookupEnv("TENANTID")

	host := "https://dev.digit.org/"
	tenantId := "pb"

	url := host + "egov-mdms-service/v1/_search"

	method := "POST"

	requestBody := `{
		"RequestInfo": {
			"authToken": ""
		},
		"MdmsCriteria": {
			"tenantId": "{{TENANTID}}",
			"moduleDetails": [
				{
					"moduleName": "zuul",
					"masterDetails": [
						{
							"name": "RateLimiting"
						}
					]
				}
			]
		}
	}`

	requestBody = strings.Replace(requestBody, "{{TENANTID}}", tenantId, 1)

	payload := strings.NewReader(requestBody)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		fmt.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return
	}

	var data map[string]interface{}
	errUnMarshalling := json.Unmarshal([]byte(body), &data)

	if errUnMarshalling != nil {
		panic(errUnMarshalling)
	}

	rateLimitingURLs := data["MdmsRes"].(map[string]interface{})["zuul"].(map[string]interface{})["RateLimiting"]

	fmt.Println("var1 = ", reflect.TypeOf(rateLimitingURLs))

	l := getLimiters(rateLimitingURLs.([]interface{}))

	writeLimiterToFile(l)

}

func getLimiters(maps []interface{}) (l *[]Limiter) {
	limiters := []Limiter{}
	for _, val := range maps {
		url := val.(map[string]interface{})["url"].(string)
		limitingType := val.(map[string]interface{})["type"].(string)
		quota := val.(map[string]interface{})["quota"].(float64)
		refreshInterval := val.(map[string]interface{})["refresh-interval"].(float64)
		limit := val.(map[string]interface{})["rate"].(float64)
		service := val.(map[string]interface{})["service"].(string)
		limiters = append(limiters, Limiter{url, limitingType, limit, quota, refreshInterval, service})
	}
	return &limiters
}

// Writes configuration for rate limiting urls
// Constant properties like type of DB are also added
func writeLimiterToFile(l *[]Limiter) {
	path, _ := os.LookupEnv("LIMITER_FILE_PATH")
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}

	_, errFixedString := f.WriteString(fixedProperties)

	if errFixedString != nil {
		panic(errFixedString)
	}

	tmpl, err := template.New("test").Parse(limiterTemplate)
	if err != nil {
		panic(err)
	}

	err = tmpl.Execute(f, *l)
	if err != nil {
		panic(err)
	}

	f.Close()
}
