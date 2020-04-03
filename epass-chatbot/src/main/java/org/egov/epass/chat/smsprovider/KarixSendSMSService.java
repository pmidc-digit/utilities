package org.egov.epass.chat.smsprovider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.WriteContext;
import lombok.extern.slf4j.Slf4j;
import org.egov.epass.chat.model.Sms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;

@Slf4j
@Service
public class KarixSendSMSService {

    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private ObjectMapper objectMapper;

    @Value("${karix.sms.service.url}")
    private String karixSmsServiceUrl;
    @Value("${karix.auth.token}")
    private String karixAuthToken;
    @Value("${karix.sender.id}")
    private String karixSenderId;
    @Value("${karix.send.sms.enabled}")
    private boolean karixSendSmsEnabled;

    private String karixSendSmsRequestBody = "{\"ver\":\"1.0\",\"key\":\"\",\"messages\":[{\"dest\":[\"\"],\"text\":\"\",\"send\":\"\"}]}";

    public void sendSMS(Sms sms) throws IOException {

        WriteContext request = JsonPath.parse(karixSendSmsRequestBody);

        request = fillCredentials(request);

        request.set("$.messages.[0].dest.[0]", sms.getMobileNumber());
        request.set("$.messages.[0].text", sms.getText());

        JsonNode requestJson = objectMapper.readTree(request.jsonString());

        log.info("Trying to send an sms");

        if(karixSendSmsEnabled) {
            ResponseEntity<String> responseEntity = restTemplate.postForEntity(karixSmsServiceUrl, requestJson,
                    String.class);
            log.info("Response from Karix : " + responseEntity.getBody());
        }
    }

    private WriteContext fillCredentials(WriteContext request) {
        request.set("$.key", karixAuthToken);
        request.set("$.messages.[0].send", karixSenderId);
        return request;
    }

}
