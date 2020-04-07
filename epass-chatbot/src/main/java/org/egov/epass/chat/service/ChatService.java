package org.egov.epass.chat.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.WriteContext;
import lombok.extern.slf4j.Slf4j;
import org.egov.epass.chat.model.Sms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Service
public class ChatService {

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${error.message.for.not.recognized}")
    private String errorMessageForNotRecognized;
    @Value("${error.message.for.server.error}")
    private String errorMessageForServerError;
    @Value("${message.verify.response}")
    private String messageVerifyResponse;

    @Value("${send.message.topic}")
    private String sendMessageTopic;

    @Value("${epass.service.host}")
    private String epassServiceHost;
    @Value("${epass.service.search.path}")
    private String epassServiceSearchPath;

    private String searchRequestBody = "{\"token\":\"\"}";

    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private KafkaTemplate<String, JsonNode> kafkaTemplate;

    public JsonNode getSmsForMessage(JsonNode chatNode) {

        String mobileNumber = chatNode.get("mobileNumber").asText();
        Sms sms = Sms.builder().mobileNumber(mobileNumber).build();

        String messageContent = chatNode.get("messageContent").asText();
        String token = extractToken(messageContent);


        if(token.isEmpty()) {
            sms.setText(errorMessageForNotRecognized);
        } else {
            try {
                WriteContext request = JsonPath.parse(searchRequestBody);
                request.set("$.token", token);
                JsonNode requestJson = objectMapper.readTree(request.jsonString());

                ResponseEntity<JsonNode> responseEntity = restTemplate.postForEntity(epassServiceHost + epassServiceSearchPath,
                        requestJson, JsonNode.class);

                if (responseEntity.getStatusCode().is2xxSuccessful()) {
                    String message = getSmsText(responseEntity.getBody());
                    sms.setText(message);
                } else {
                    sms.setText(errorMessageForServerError);
                }
            } catch (Exception e) {
                sms.setText(errorMessageForServerError);
                log.error("Error while calling ecurfew service ", e);
            }
        }

        JsonNode smsJson = objectMapper.convertValue(sms, JsonNode.class);

        return smsJson;
    }

    String getSmsText(JsonNode epass) {
        String message =  messageVerifyResponse;

        String firstName = epass.at("/entity/firstName").asText();
        message = message.replaceAll("<firstName>", firstName);
        String lastName = epass.at("/entity/lastName").asText();
        message = message.replaceAll("<lastName>", lastName);
        String token = epass.at("/token").asText();
        message = message.replaceAll("<token>", token);
        String validLocations = epass.at("/validLocations").asText();
        message = message.replaceAll("<validLocations>", validLocations);

        Long endTime = epass.at("/endTime").asLong();
        String validTillDate = getDateFromTimestamp(endTime);
        message = message.replaceAll("<validTillDate>", validTillDate);

        return message;
    }

    private String getDateFromTimestamp(Long timestamp) {
        DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
        return dateFormat.format(timestamp);
    }

    private String extractToken(String message) {
        List<String> words = Arrays.asList(message.split(" "));

        if(words.size() == 0) {
            log.info("Empty message");
            return "";
        }

        if(!words.get(0).equalsIgnoreCase("Verify")) {
            log.info("First word is not Verify. Still proceeding");
        }

        if(words.size() == 1) {
            log.info("Single word received. Assuming it to be token");
            return words.get(0);
        }

        return words.get(1);
    }

}
