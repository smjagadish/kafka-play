package com.example.kafkademo;

import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@SpringBootApplication
public class KafkademoApplication {


	public static void main(String[] args) {

		ApplicationContext ctx = SpringApplication.run(KafkademoApplication.class, args);
		messageSender obj = ctx.getBean(messageSender.class);
		obj.send("sample-topic4"," testvalue");

	}
	@Component
	private class messageSender{

		@Autowired
		KafkaTemplate <String,Object> kt;
		@Autowired
		KafkaTemplate <String,String> ktString;

		private void send(String topic , String val)
		{
			String serializer = kt.getProducerFactory().getConfigurationProperties().get("bootstrap.servers").toString();
			System.out.println(serializer);
			System.out.println(ktString.getProducerFactory().getConfigurationProperties().get("client.id").toString());
			//kt.execute();

			ListenableFuture<SendResult<String, Object>> future = kt.send(topic,  val);
			future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
				@Override
				public void onFailure(Throwable ex) {

				}

				@Override
				public void onSuccess(SendResult<String, Object> result) {
                    System.out.println("wrote date with" +" "+ "value:"+" "+result.getProducerRecord().value().toString()+ " "+ " to the topic:" + " " +result.getRecordMetadata().topic());
				}
			});
			}
		}
	}



