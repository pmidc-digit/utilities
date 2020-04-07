package org.egov.epass.chat.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.egov.epass.chat.model.Sms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

@Component
public class EpassCreateNotification {

    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private KafkaTemplate<String, JsonNode> kafkaTemplate;

    @Value("${send.message.topic}")
    private String sendMessageTopic;

    @Value("${message.epass.create}")
    private String messageTemplateForCreatePass;

    public JsonNode getSmsForCreatedPass(JsonNode epass) {
        String mobileNumber = epass.at("/entity/phoneNumber").asText().substring(3);

        Sms sms = Sms.builder().mobileNumber(mobileNumber).build();

        String text = getSmsText(epass);

        sms.setText(text);

        JsonNode smsJson = objectMapper.convertValue(sms, JsonNode.class);

        return smsJson;
    }

    private String getSmsText(JsonNode epass) {
        String message = messageTemplateForCreatePass;

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


}
