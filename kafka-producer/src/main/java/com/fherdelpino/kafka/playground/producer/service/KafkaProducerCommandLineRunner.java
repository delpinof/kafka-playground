package com.fherdelpino.kafka.playground.producer.service;

import com.fherdelpino.kafka.playground.common.avro.model.Student;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaProducerCommandLineRunner implements CommandLineRunner {

    @Value("${kafka.string-topic-name}")
    private String stringTopicName;

    @Value("${kafka.student-avro-topic-name}")
    private String studentAvroTopicName;

    @Autowired
    private Producer<String, String> kafkaStringProducer;

    @Autowired
    private Producer<String, Student> kafkaAvroProducer;

    @Override
    public void run(String... args) throws Exception {
        for (int i = 0; true; i++) {
            sendStudentAvro(i);
            log.info("Message sent successfully!");
            Thread.sleep(1000);
        }
    }

    private void sendString(int i) throws InterruptedException {
        String message = String.format("message %d", i);
        kafkaStringProducer.send(new ProducerRecord<>(stringTopicName, message));
    }

    private void sendStudentAvro(int i) throws InterruptedException {
        Student student = Student.newBuilder()
                .setStudentId("s" + i)
                .setAge(18)
                .setStudentName("Fernando")
                .build();
        kafkaAvroProducer.send(new ProducerRecord<>(studentAvroTopicName, student));
    }
}
