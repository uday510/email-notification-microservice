package com.app.emailnotification.handler;

import com.app.core.ProductCreateEvent;
import com.app.emailnotification.error.NonRetryableException;
import com.app.emailnotification.error.RetryableException;
import com.app.emailnotification.io.ProcessedEventEntity;
import com.app.emailnotification.io.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

    private final RestTemplate restTemplate;

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    private final ProcessedEventRepository processedEventRepository;

    public ProductCreatedEventHandler(RestTemplate restTemplate, ProcessedEventRepository processedEventRepository) {
        this.restTemplate = restTemplate;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    @KafkaHandler
    public void handle(
            @Payload ProductCreateEvent event,
            @Header(value = "messageId", required = false) String messageId,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp
    ) {
        LOGGER.info("""
        Received Product Event:
        ├── title: {}
        ├── productId: {}
        ├── messageKey: {}
        ├── messageId: {}
        ├── topic: {}
        ├── partition: {}
        ├── offset: {}
        └── timestamp: {}
        """,
                event.getTitle(),
                event.getProductId(),
                key,
                messageId,
                topic,
                partition,
                offset,
                timestamp
        );

        ProcessedEventEntity existingRecord = processedEventRepository.findByMessageId(messageId);
        if (existingRecord != null) {
            LOGGER.info("Found a duplicate message id: {}", existingRecord.getMessageId());
        }

        final String URL = "http://localhost:8082/response/200";
        try {
            ResponseEntity<String> response = restTemplate.exchange(URL, HttpMethod.GET, null, String.class);

            if (response.getStatusCode().value() == HttpStatus.OK.value()) {
                LOGGER.info("Received response from remote service: {}", response.getBody());
            }
        } catch (ResourceAccessException e) {
            LOGGER.error("Network issue: {}", e.getMessage());
            throw new RetryableException(e);
        } catch (HttpServerErrorException e) {
            LOGGER.error("Server error ({}): {}", e.getStatusCode(), e.getMessage());
            throw new NonRetryableException(e);
        } catch (Exception e) {
            LOGGER.error("Unexpected error: {}", e.getMessage());
            throw new NonRetryableException(e);
        }

        try {
            processedEventRepository.save(new ProcessedEventEntity(messageId, event.getProductId()));
        } catch (DataIntegrityViolationException e) {
            throw new NonRetryableException(e);
        }
    }
}