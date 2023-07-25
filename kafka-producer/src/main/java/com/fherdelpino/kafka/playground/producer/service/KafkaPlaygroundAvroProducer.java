package com.fherdelpino.kafka.playground.producer.service;

import com.fherdelpino.kafka.playground.common.avro.model.Student;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@ConditionalOnProperty(prefix = "playground", name = "producer-type", havingValue = "avro")
public class KafkaPlaygroundAvroProducer implements InfiniteProducer {

    @Value("${kafka.student-avro-topic-name}")
    private String studentAvroTopicName;

    @Autowired
    private Producer<String, Student> kafkaAvroProducer;

    @Override
    public void produce() throws Exception {
        for (int i = 0; true; i++) {
            Student student = Student.newBuilder()
                    .setStudentId("s" + i)
                    .setAge(18)
                    .setStudentName("Fernando")
                    .build();
            kafkaAvroProducer.send(new ProducerRecord<>(studentAvroTopicName, student));
            log.info("Message sent successfully!");
            Thread.sleep(1000);
        }
    }
}
