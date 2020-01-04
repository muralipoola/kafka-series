package kafka.series.learning.apache.kafka.controller;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("producer")
public class ProducerController {

  Logger logger = LoggerFactory.getLogger(ProducerController.class);
  String KafkaTopicName = "learning.apache.kafka";
  String KafkaServers = "localhost:9092";

  @RequestMapping(method = RequestMethod.GET)
  public String get(){
    return "Application is up and running successfully...";
  }

  @RequestMapping(method = RequestMethod.POST)
  public void produce(@RequestBody String message){
    logger.info("Message received: [{}]", message);

    Properties producerProperties = getProducerProperties();
    Producer<String, String> producer = new KafkaProducer<>(producerProperties);

    producer.send(new ProducerRecord<>(KafkaTopicName, message));
    producer.flush();
    producer.close();
  }

  private Properties getProducerProperties() {
    Properties producerProperties = new Properties();

    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaServers);
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return producerProperties;
  }

}
