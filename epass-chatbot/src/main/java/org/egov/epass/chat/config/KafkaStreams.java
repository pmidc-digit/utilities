package org.egov.epass.chat.config;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.egov.epass.chat.service.ChatService;
import org.egov.epass.chat.service.EpassCreateNotification;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.Properties;

@Configuration
public class KafkaStreams {

    @Autowired
    private KafkaStreamsConfig kafkaStreamsConfig;

    @Value("${send.message.topic}")
    private String sendMessageTopicName;

    @Autowired
    private ChatService chatService;
    private String chatServiceStreamName = "epass-chat-stream";
    private String receivedMessageTopicName = "karix-received-messages";

    @Autowired
    private EpassCreateNotification epassCreateNotification;
    private String epassNotificationsStreamName = "epass-notifications-stream";
    @Value("${epass.notifications.topic}")
    private String epassNotificationsTopic;


    @PostConstruct
    public void initChatServiceKStream() {

        Properties streamConfiguration = kafkaStreamsConfig.getDefaultStreamConfiguration();
        streamConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, chatServiceStreamName);
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, JsonNode> messagesKStream = builder.stream(receivedMessageTopicName, Consumed.with(Serdes.String(),
                kafkaStreamsConfig.getJsonSerde()));

        messagesKStream.flatMapValues(chatNode -> {
            try {
                return Collections.singletonList(chatService.getSmsForMessage(chatNode));
            } catch (Exception e) {
                return Collections.emptyList();
            }
        }).to(sendMessageTopicName, Produced.with(Serdes.String(), kafkaStreamsConfig.getJsonSerde()));

        kafkaStreamsConfig.startStream(builder, streamConfiguration);
    }

    @PostConstruct
    public void initEpassNotificationsKStream() {

        Properties streamConfiguration = kafkaStreamsConfig.getDefaultStreamConfiguration();
        streamConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, epassNotificationsStreamName);
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, JsonNode> messagesKStream = builder.stream(epassNotificationsTopic, Consumed.with(Serdes.String(),
                kafkaStreamsConfig.getJsonSerde()));

        messagesKStream.flatMapValues(notification -> {
            try {
                return Collections.singletonList(epassCreateNotification.getSmsForCreatedPass(notification));
            } catch (Exception e) {
                return Collections.emptyList();
            }
        }).to(sendMessageTopicName, Produced.with(Serdes.String(), kafkaStreamsConfig.getJsonSerde()));

        kafkaStreamsConfig.startStream(builder, streamConfiguration);
    }

}
