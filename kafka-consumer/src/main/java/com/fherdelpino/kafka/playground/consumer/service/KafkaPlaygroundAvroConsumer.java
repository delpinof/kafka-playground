package com.fherdelpino.kafka.playground.consumer.service;

import com.fherdelpino.kafka.playground.common.avro.model.Student;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;

@Slf4j
@Service
@ConditionalOnProperty(prefix = "playground", name = "consumer-type", havingValue = "avro")
public class KafkaPlaygroundAvroConsumer implements KafkaPlaygroundConsumer {

    @Value("${kafka.student-avro-topic-name}")
    private String studentAvroTopicName;

    @Autowired
    private Consumer<String, Student> kafkaStudentAvroConsumer;

    @Override
    public void consume() {
        kafkaStudentAvroConsumer.subscribe(Collections.singletonList(studentAvroTopicName));
        while(true) {
            kafkaStudentAvroConsumer.poll(Duration.ofMillis(100))
                    .forEach(record -> log.info("{}", record.value()));
        }
    }
}
