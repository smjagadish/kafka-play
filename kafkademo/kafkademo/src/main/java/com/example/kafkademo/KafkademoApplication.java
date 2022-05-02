package com.example.kafkademo;

import com.example.kafkaconfig.KafkaConfiguration;
import org.apache.kafka.clients.producer.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
		@Autowired
		DefaultKafkaProducerFactory<String,Object> pf;

		private void send(String topic , String val)
		{
			String serializer = kt.getProducerFactory().getConfigurationProperties().get("bootstrap.servers").toString();
			System.out.println(serializer);
			System.out.println(ktString.getProducerFactory().getConfigurationProperties().get("client.id").toString());
			System.out.println("test out");
			kt.setProducerListener(new ProducerListener<String, Object>() {
				void onSuccess(ProducerRecord<String,Object> data)
				{

				}
				void onError(ProducerRecord<String,Object> data)
				{
					System.out.println("errored out");
				}
			});
			kt.execute(new KafkaOperations.ProducerCallback<String, Object, Object>() {

				@Override
				public Object doInKafka(Producer<String, Object> producer) {
					Future<RecordMetadata> res=null;
					try {
						System.out.println("test send out");
						for(int i=0;i<10;i++)
					 	producer.send(new ProducerRecord<>(topic, val));

					}
					catch(Exception e)
					{
						System.out.println(e);
						e.printStackTrace();
					}

					return null;
				}
			});
            //pf.destroy();
			//pf.setProducerPerThread(true);
            pf.updateConfigs(Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG,"changedproducer")); // producer config can be updated on the fly . existing  producers need a reset though
			pf.reset(); // closing producer to reflect config change


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



