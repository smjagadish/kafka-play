package com.example.kafkaconfig;

import com.example.pojo.userInfo;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

@Configuration
public class KafkaConfiguration {

    @Autowired
    public KafkaProperties kafkaProperties;

    @Bean
    public ProducerFactory<Object, Object> producerFactory() {
        /*Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.CLIENT_ID_CONFIG , "Objproducer");
        config.put(ProducerConfig.ACKS_CONFIG,"all");
        //config.put(ProducerConfig.ACKS_CONFIG , kafkaProperties.getProperties().get("spring.kafka.producer.acks"));*/

        return new DefaultKafkaProducerFactory<>(config_src0());
    }
    @Bean
    public ProducerFactory<Object, Object> duplicateproducerFactory() {
        /*Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.CLIENT_ID_CONFIG , "Objproducer");
        config.put(ProducerConfig.ACKS_CONFIG,"all");
        //config.put(ProducerConfig.ACKS_CONFIG , kafkaProperties.getProperties().get("spring.kafka.producer.acks"));*/
        return new DefaultKafkaProducerFactory<>(config_src0());
    }
    @Bean(name="factory")
    @Primary
    public ProducerFactory<String, String> stringProducerFactory() {
      /*  Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.CLIENT_ID_CONFIG , "stringproducer");*/
        //config.put(ProducerConfig.ACKS_CONFIG , kafkaProperties.getProperties().get("spring.kafka.producer.acks"));
        return new DefaultKafkaProducerFactory<>(config_src1());
    }
@Bean
public Map<String ,Object> config_src1()
{
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.CLIENT_ID_CONFIG , "stringproducer");
    return config;
}
    @Bean
    public Map<String ,Object> config_src0()
    {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.CLIENT_ID_CONFIG , "stringproducer");
        return config;
    }

    @Bean
    public Map<String,Object> config_src2()
    {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      //  config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.CLIENT_ID_CONFIG , "jsonproducer");
        return config;
    }

    @Bean(name="json")
    public KafkaTemplate<String, userInfo> pojoKafkaTemplate(){
return new KafkaTemplate<String,userInfo>(pojoProducerFactory());
    }

    @Bean
    public ProducerFactory<String, userInfo> pojoProducerFactory() {
        return new DefaultKafkaProducerFactory<>(config_src2(),null,()-> new JsonSerializer());
    }

    @Bean(name="kt")
    public KafkaTemplate<Object, Object> kafkaTemplate() {
        return new KafkaTemplate<Object , Object>(producerFactory());

    }
    @Bean(name="duplicatekt")
    public KafkaTemplate<Object, Object> duplicatekafkaTemplate() {
        return new KafkaTemplate<Object , Object>(duplicateproducerFactory());

    }
    @Bean(name="ktstring")
    public KafkaTemplate<String, String> stringTemplate() {
        return new KafkaTemplate<String, String>(stringProducerFactory());
    }
    @Bean(name="ktbyte")
    @Qualifier("factory")
    public KafkaTemplate<String, byte[]> stringTemplate2(ProducerFactory<String, byte[]> pf) {
        return new KafkaTemplate<String, byte[]>(pf, Collections.singletonMap(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
    }
    @Bean
    public RoutingKafkaTemplate routingTemplate()
    {
        Map<Pattern,ProducerFactory<Object,Object>> rmap= new LinkedHashMap<>();
        rmap.put(Pattern.compile("sample-topic4"),producerFactory());
        rmap.put(Pattern.compile("dummy"),duplicateproducerFactory());
        return new RoutingKafkaTemplate(rmap);
    }

    @Bean
    public NewTopic sampleTopic() {
        return new NewTopic("sample-topic4", 1, (short) 1);
    }
}
