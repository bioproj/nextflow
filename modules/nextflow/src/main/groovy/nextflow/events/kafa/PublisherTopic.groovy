package nextflow.events.kafa

import groovy.transform.CompileStatic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer


/**
 * A class helper to publish messages in a topic
 *
 *
 * @author : jorge <jorge.aguilera@seqera.io>
 *
 */
@CompileStatic
class PublisherTopic {

    private String url
    private String group
    private String topic

    PublisherTopic withGroup(String group) {
        this.group = group
        this
    }

    PublisherTopic withUrl(String url) {
        this.url = url
        this
    }

    PublisherTopic withTopic(String topic) {
        this.topic = topic
        this
    }

    private KafkaProducer<String,String>createProducer(){
        Thread.currentThread().setContextClassLoader(null)
        Properties properties = new Properties()
        properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = url
        properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer.class.name
        properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer.class.name
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties)
        producer
    }

    void publishMessage(Object message){
        KafkaProducer<String,String> producer = createProducer()
        ProducerRecord<String,String> record
        if( message instanceof List) {
            def list = message as List
            record = new ProducerRecord<>(topic, list[0].toString(), list[1].toString())
        }else {
            record = new ProducerRecord<>(topic, message.toString())
        }
        producer.send(record)
        producer.close()
    }

}
