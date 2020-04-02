package org.egov.epass.chat.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.WriteContext;
import lombok.extern.slf4j.Slf4j;
import org.egov.epass.chat.model.Sms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Service
public class KarixSendSMSService {

    @Autowired
    private RestTemplate restTemplate;

    @Value("${karix.sms.service.url}")
    private String karixSmsServiceUrl;
    @Value("${karix.auth.token}")
    private String karixAuthToken;

    private String karixSendSmsRequestBody = "{\"ver\":\"1.0\",\"key\":\"\",\"encrpt\":\"0\",\"messages\":[{\"dest\":[\"\"],\"text\":\"\"}]}";

    public void sendSMS(Sms sms) {

        WriteContext request = JsonPath.parse(karixSendSmsRequestBody);

        request.set("$.messages.[0].dest.[0]", sms.getMobileNumber());
        request.set("$.messages.[0].text", sms.getText());

        ResponseEntity<JsonNode> responseEntity = restTemplate.postForEntity(karixSmsServiceUrl, request.json(),
                JsonNode.class);

        log.debug(responseEntity.getBody().toString());
    }

    private WriteContext fillCredentials(WriteContext request) {
        request.set("$.key", karixAuthToken);
        return request;
    }

}
