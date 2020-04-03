package org.egov.epass.chat.smsprovider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.WriteContext;
import lombok.extern.slf4j.Slf4j;
import org.egov.epass.chat.model.Sms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;

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
    @Value("${karix.sms.max.batch.size}")
    private Integer karixMaxBatchSize;

    private String karixSendSmsRequestBody = "{\"ver\":\"1.0\",\"key\":\"\",\"messages\":[{\"dest\":[\"\"],\"text\":\"\",\"send\":\"\"}]}";
    private String karixMessageBody = "{\"dest\":[\"\"],\"text\":\"\",\"send\":\"\"}";

    @PostConstruct
    public void initWithCredentials() {
        WriteContext request = JsonPath.parse(karixSendSmsRequestBody);
        request.set("$.key", karixAuthToken);
        karixSendSmsRequestBody = request.jsonString();

        WriteContext message = JsonPath.parse(karixMessageBody);
        message.set("$.send", karixSenderId);
        karixMessageBody = message.jsonString();
    }

    public void sendSMS(List<Sms> smsList) throws IOException {
        if(smsList.size() <= karixMaxBatchSize) {
            sendSmsBatch(smsList);
        } else {
            int i = 0;
            while (i < smsList.size()) {
                int endIndex = i + karixMaxBatchSize < smsList.size() ? i + karixMaxBatchSize : smsList.size();
                List<Sms> smsSubList = smsList.subList(i, endIndex);
                sendSmsBatch(smsSubList);
                i = endIndex;
            }
        }
    }

    public void sendSmsBatch(List<Sms> smsList) throws IOException {
        ArrayNode messages = objectMapper.createArrayNode();

        for(Sms sms : smsList) {
            ArrayNode destinationMobileNumbers = objectMapper.createArrayNode();
            destinationMobileNumbers.add(sms.getMobileNumber());

            ObjectNode message = (ObjectNode) objectMapper.readTree(karixMessageBody);
            message.put("text", sms.getText());
            message.put("dest", destinationMobileNumbers);

            messages.add(message);
        }

        ObjectNode request = (ObjectNode) objectMapper.readTree(karixSendSmsRequestBody);

        request.set("messages", messages);

        log.info("Trying to send an sms");

        log.info("Request : " + request.toString());

        if(karixSendSmsEnabled) {
            ResponseEntity<String> responseEntity = restTemplate.postForEntity(karixSmsServiceUrl, request,
                    String.class);
            log.info("Response from Karix : " + responseEntity.getBody());
        }
    }
}
