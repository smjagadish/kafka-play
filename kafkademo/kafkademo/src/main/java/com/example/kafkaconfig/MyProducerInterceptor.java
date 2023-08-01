package com.example.kafkaconfig;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

public class MyProducerInterceptor implements org.apache.kafka.clients.producer.ProducerInterceptor {
    @Autowired
    processBean pBean;
    public MyProducerInterceptor()
    {

    }
    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        pBean.intercept();
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        pBean.custom();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
   this.pBean = (processBean) map.get("my.bean");
    }
}
