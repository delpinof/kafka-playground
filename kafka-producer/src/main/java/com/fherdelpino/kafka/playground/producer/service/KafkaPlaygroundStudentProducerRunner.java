package com.fherdelpino.kafka.playground.producer.service;

import com.fherdelpino.kafka.playground.common.avro.model.Student;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@ConditionalOnProperty(prefix = "playground", name = "producer-type", havingValue = "student")
public class KafkaPlaygroundStudentProducerRunner implements CommandLineRunner {

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    private Producer<String, SpecificRecord> kafkaAvroProducer;

    @Override
    public void run(String... args) throws Exception {
        for (int i = 0; true; i++) {
            Student student = Student.newBuilder()
                    .setStudentId("s" + i)
                    .setAge(18)
                    .setStudentName("Fernando")
                    .build();
            kafkaAvroProducer.send(new ProducerRecord<>(topic, student));
            log.info("{} sent successfully!", student);
            Thread.sleep(1000);
        }
    }
}
