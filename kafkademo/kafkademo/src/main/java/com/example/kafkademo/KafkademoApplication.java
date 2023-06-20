package com.example.kafkademo;

import com.example.kafkaconfig.KafkaConfiguration;
import com.example.pojo.userInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@SpringBootApplication
@ComponentScan("com.example.kafkaconfig")
public class KafkademoApplication {


	public static void main(String[] args) {

		ApplicationContext ctx = SpringApplication.run(KafkademoApplication.class, args);
		messageSender obj = ctx.getBean(messageSender.class);
		obj.send("sample-topic4", " testvalue");


	}

	@Component
	private class messageSender {


		@Autowired
		KafkaTemplate<Object , Object> kt;
		@Autowired
		@Qualifier("duplicatekt")
		KafkaTemplate<Object , Object> kt_dup;
		@Autowired
		KafkaTemplate<String, String> ktString;
		@Autowired
		@Qualifier("factory")
		DefaultKafkaProducerFactory<String, Object> pf;
		@Autowired
		KafkaTemplate<String , byte[]> barray;
		@Autowired
		RoutingKafkaTemplate ktroute;
		@Autowired
		KafkaTemplate<String, userInfo> ktpojo;

		private void send(String topic, String val) {
			pf.addListener(new ProducerFactory.Listener<String, Object>() {
				@Override
				public void producerAdded(String id, Producer<String, Object> producer) {
					System.out.println("producer added with id" +id);
				}

				@Override
				public void producerRemoved(String id, Producer<String, Object> producer) {
					System.out.println("producer removed");
				}
			});
			String serializer = kt.getProducerFactory().getConfigurationProperties().get("bootstrap.servers").toString();
			System.out.println(serializer);
			System.out.println(ktString.getProducerFactory().getConfigurationProperties().get("client.id").toString());
			System.out.println("test out");
			kt.setProducerListener(new ProducerListener<Object, Object>() {

				void onSuccess(String topic, Integer partition, String key, Object value,
							   RecordMetadata recordMetadata)
				{
					System.out.println("sucessful listerner call out");
				}
			});
	/*		kt.execute(new KafkaOperations.ProducerCallback<Object, Object, Object>() {

				@Override
				public Object doInKafka(Producer<Object, Object> producer) {
					Future<RecordMetadata> res = null;
					try {
						System.out.println("test send out");
						for (int i = 0; i < 10; i++)
							producer.send(new ProducerRecord<>(topic, val));


					} catch (Exception e) {
						System.out.println(e);
						e.printStackTrace();
					}

					return null;
				}
			}); */
//barray.send(topic,val.getBytes(StandardCharsets.UTF_8));

		//	barray.send(topic,"textdata".getBytes());
			pf.setProducerPerThread(true);
			pf.updateConfigs(Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, "changedproducer")); // producer config can be updated on the fly . existing  producers need a reset though
			pf.reset(); // closing producer to reflect config change

	/*		ListenableFuture<SendResult<Object, Object>> future = kt.send(topic, val);

			future.addCallback(new ListenableFutureCallback<SendResult<Object, Object>>() {
				@Override
				public void onFailure(Throwable ex) {

				}

				@Override
				public void onSuccess(SendResult<Object, Object> result) {
					System.out.println("wrote date with" + " " + "value:" + " " + result.getProducerRecord().value().toString() + " " + " to the topic:" + " " + result.getRecordMetadata().topic());
				}
			});
*/
		ktroute.send("dummy","1234");
	//	ktroute.send(topic,"xxxval");
		ktroute.send("dummy","hhh");
	// since ktroute kafka template packs a value type of object, the below instruction goes through
	// lombok generated toString will give a meaningful representation instead of the default obj.tostring()
		ktroute.send("dummy",userInfo.builder().dept("CS").name("dumb user").id(345).build());
		//changes for json serializer
			userInfo uinfo = userInfo.builder().dept("IT").name("text user").id(145).build();
           System.out.println(uinfo.toString());
			ktpojo.send(topic,uinfo);

			//Jun20 , 2023
        // injecting a poison pill on purpose
			ktroute.send(topic,"junk data");
	    // poison pill ends

        // send using message object. use this to set custom headers so as to use delegating serializer as a check
			Message<?> msg = MessageBuilder.withPayload(uinfo)
					.setHeader("DelegatingSerializer.VALUE_SERIALIZATION_SELECTOR ","ser1")
					.setHeader(KafkaHeaders.TOPIC,"dummy")
					.build();
             kt_dup.send(msg);
		}
	}
}



