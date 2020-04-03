package org.egov.epass.chat.web.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.egov.tracer.kafka.CustomKafkaTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Map;
import java.util.UUID;

@Slf4j
@Controller
public class EpassChatController {

    @Autowired
    private KafkaTemplate<String, JsonNode> kafkaTemplate;
    @Autowired
    private ObjectMapper objectMapper;

    private String receivedMessageTopicName = "karix-received-messages";

    @RequestMapping(value = "/messages", method = RequestMethod.GET)
    public ResponseEntity<JsonNode> receiveMessage(@RequestParam Map<String, String> params) throws Exception {

        String mobileNumber = params.get("Send").substring(2);
        String messageContent = params.get("Rawmessage");
        String recipientNumber = params.get("VN");
        String stime = params.get("MID");

        ObjectNode extraInfo = objectMapper.createObjectNode();
        extraInfo.put("MID", stime);
        extraInfo.put("recipient", recipientNumber);

        ObjectNode chatNode = objectMapper.createObjectNode();
        chatNode.put("timestamp", System.currentTimeMillis());
        chatNode.put("mobileNumber", mobileNumber);
        chatNode.put("messageContent", messageContent);
        chatNode.set("extraInfo", extraInfo);

        kafkaTemplate.send(receivedMessageTopicName, mobileNumber, chatNode);

        JsonNode response = createResponse();

        log.info("Received an sms, replied with : " + response.toString());

        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    private JsonNode createResponse() {
        ObjectNode response = objectMapper.createObjectNode();
        response.put("id", UUID.randomUUID().toString());
        response.put("timestamp", System.currentTimeMillis());
        return response;
    }

}
