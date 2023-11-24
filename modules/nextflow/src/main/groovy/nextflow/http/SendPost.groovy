package nextflow.http

import com.google.gson.Gson
import com.google.gson.JsonObject
import groovy.transform.CompileStatic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets

@CompileStatic
class SendPost {
    private String url
    private String group
    private String topic

    SendPost withGroup(String group) {
        this.group = group
        this
    }

    SendPost withUrl(String url) {
        this.url = url
        this
    }

    SendPost withTopic(String topic) {
        this.topic = topic
        this
    }

//    private KafkaProducer<String,String> createProducer(){
//        Thread.currentThread().setContextClassLoader(null)
//        Properties properties = new Properties()
//        properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = url
//        properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer.class.name
//        properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer.class.name
//        KafkaProducer<String,String> producer = new KafkaProducer<>(properties)
//        producer
//    }

    void sendPost(Object message){
//        KafkaProducer<String,String> producer = createProducer()
//        ProducerRecord<String,String> record
        if( message instanceof LinkedHashMap) {
            def link = message as LinkedHashMap

            Gson gson = new Gson();
            String json = gson.toJson(link);
            String url = this.url+"/${message["id"]}";
//            String postData = "key="+message[""]+"&key2=value2";

            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            int responseCode = response.statusCode();
            System.out.println("Response Code: " + responseCode);

            // 读取响应内容
            String responseBody = response.body();
            System.out.println("Response: " + responseBody);
//            record = new ProducerRecord<>(topic, list[0].toString(), list[1].toString())
        }else {
//            record = new ProducerRecord<>(topic, message.toString())
        }



//        producer.send(record)
//        producer.close()
    }


}

