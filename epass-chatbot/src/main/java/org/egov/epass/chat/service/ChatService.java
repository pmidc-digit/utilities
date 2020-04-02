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

    public void processMessage(String kafkaKey, JsonNode chatNode) throws IOException {
        log.info("ChatNode : " + chatNode.toString());

        String mobileNumber = chatNode.get("mobileNumber").asText();
        Sms sms = Sms.builder().mobileNumber(mobileNumber).build();

        String messageContent = chatNode.get("messageContent").asText();
        String token = extractToken(messageContent);


        if(token.isEmpty()) {
            sms.setText(errorMessageForNotRecognized);
        } else {

            WriteContext request = JsonPath.parse(searchRequestBody);
            request.set("$.token", token);

            JsonNode requestJson = objectMapper.readTree(request.jsonString());
            try {
                ResponseEntity<JsonNode> responseEntity = restTemplate.postForEntity(epassServiceHost + epassServiceSearchPath,
                        requestJson, JsonNode.class);

                if (responseEntity.getStatusCode().is2xxSuccessful()) {
                    String message = populateTemplateMessage(messageVerifyResponse, responseEntity.getBody());
                    sms.setText(message);
                } else {
                    sms.setText(errorMessageForServerError);
                }
            } catch (Exception e) {
                sms.setText(errorMessageForServerError);
                log.error("Error while calling epass service ", e);
            }
        }

        JsonNode smsJson = objectMapper.convertValue(sms, JsonNode.class);
        kafkaTemplate.send(sendMessageTopic, kafkaKey, smsJson);
    }

    private String populateTemplateMessage(String template, JsonNode searchResponse) {

        // TODO : Populate template message

        return template;
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
