package com.example.kafkaconfig;

import com.example.pojo.userInfo;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
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
import org.springframework.kafka.support.serializer.*;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

@Configuration
public class KafkaConfiguration {

    @Autowired
    public KafkaProperties kafkaProperties;

    @Bean
    public ToStringSerializer<Object> ser() {
        return new ToStringSerializer<Object>() {

        };
    }

    @Bean
    public ProducerFactory<Object, Object> producerFactory() {
        // example of spring kafka's take of stock string serializer
        return new DefaultKafkaProducerFactory<>(config_src0(), null, ser());
    }
   /*
   @Bean

    public ProducerFactory<Object, Object> duplicateproducerFactory() {
        return new DefaultKafkaProducerFactory<>(config_src1() , null , ser());
    }
    */

    // Utilizing DelegatingByTypeSerializer instead of string serializer
    @Bean
    public ProducerFactory<Object, Object> duplicateproducerFactory() {
        return new DefaultKafkaProducerFactory<>(config_src1(), null, new DelegatingByTypeSerializer(Map.of(userInfo.class, new JsonSerializer(), String.class, new StringSerializer())));
    }

    // example for using DelegatingByTopicSerializer
/*
      @Bean
      public ProducerFactory<Object,Object> duplicateproducerFactory() {
      return new DefaultKafkaProducerFactory<>(config_src1(), null , new DelegatingByTopicSerializer(Map.of(Pattern.compile("dummy"),new JsonSerializer()));


      }
      */
    @Bean(name = "factory")
    @Primary
    public ProducerFactory<String, String> stringProducerFactory() {
        return new DefaultKafkaProducerFactory<>(config_src1(), null, new ToStringSerializer<String>() {
            public String toString() {
                return "forced content change by serializer";
            }
        });
    }

    @Bean
    public Map<String, Object> config_src1() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "stringproducer");
        config.put(DelegatingSerializer.VALUE_SERIALIZATION_SELECTOR_CONFIG, "ser1:org.springframework.kafka.support.serializer.JsonSerializer");
        return config;
    }

    @Bean
    public Map<String, Object> config_src0() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "stringproducer");
        return config;
    }

    @Bean
    public Map<String, Object> config_src2() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "jsonproducer");
        // disabling spring kafka json serializer type info
        // this allows me to use diff pojo in cons and producer
        // momentarily commenting exclusion of type info header as i rely on type mapping at deserializer side
        //config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS , "false");
        // adding type info headers which can be leveraged by deserializers
        config.put(JsonSerializer.TYPE_MAPPINGS, "uinfo:com.example.pojo.userInfo");
        // adding a producer interceptor
        config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,MyProducerInterceptor.class.getName());
        config.put("my.bean",pBean());
        return config;
    }

    @Bean(name = "json")
    public KafkaTemplate<String, userInfo> pojoKafkaTemplate() {
        return new KafkaTemplate<String, userInfo>(pojoProducerFactory());
    }

    @Bean
    public ProducerFactory<String, userInfo> pojoProducerFactory() {
        return new DefaultKafkaProducerFactory<>(config_src2(), null, () -> new JsonSerializer());
    }

    @Bean(name = "kt")
    public KafkaTemplate<Object, Object> kafkaTemplate() {
        return new KafkaTemplate<Object, Object>(producerFactory());

    }

    @Bean(name = "duplicatekt")
    public KafkaTemplate<Object, Object> duplicatekafkaTemplate() {
        return new KafkaTemplate<Object, Object>(duplicateproducerFactory());

    }

    @Bean(name = "ktstring")
    public KafkaTemplate<String, String> stringTemplate() {
        return new KafkaTemplate<String, String>(stringProducerFactory());
    }

    @Bean(name = "ktbyte")
    @Qualifier("factory")
    public KafkaTemplate<String, byte[]> stringTemplate2(ProducerFactory<String, byte[]> pf) {
        return new KafkaTemplate<String, byte[]>(pf, Collections.singletonMap(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
    }

    @Bean
    public RoutingKafkaTemplate routingTemplate() {
        Map<Pattern, ProducerFactory<Object, Object>> rmap = new LinkedHashMap<>();
        rmap.put(Pattern.compile("sample-topic4"), producerFactory());
        rmap.put(Pattern.compile("dummy"), duplicateproducerFactory());
        return new RoutingKafkaTemplate(rmap);
    }

    @Bean
    public NewTopic sampleTopic() {
        return new NewTopic("sample-topic4", 1, (short) 1);
    }

    @Bean
    public processBean pBean()
    {
        return new processBean();
    }
}
