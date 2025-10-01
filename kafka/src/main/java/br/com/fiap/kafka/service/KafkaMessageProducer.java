package br.com.fiap.kafka.service;

import jakarta.websocket.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;


@Service
public class KafkaMessageProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topicName;

    private KafkaMessageProducer(KafkaTemplate<String, String> kafkaTemplate,
                                 @Value("${app.kafka.topic.meu-topico}") String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    public void sendMessageWithKey(String key, String message){
        log.info("Sending message: '{}' com chave'{}' para o topico '{}'", message,key , topicName);
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName,key , message);
        future.whenComplete((stringStringSendResult, throwable) -> {
            if (ex == null){
                log.info("Mensagem enviada com sucesso para o topico '{}' particao '{}' com offset'{}'",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }else {
                log.error("Falha ao enviar mensagem para o topico '{}': '{}', topicName, ex.getMessage());
            }
        }
    }
}
