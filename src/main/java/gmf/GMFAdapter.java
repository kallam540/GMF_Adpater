package gmf;



import example.avro.User;
import gmf.utils.SimpleAvroSchemaFlink;
import gmf.utils.SimpleAvroSchemaJava;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class GMFAdapter {
    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.put("group.id", "customer-consumer-group-v1");
        properties.put("auto.commit.enable", "false");
        properties.put("auto.offset.reset", "earliest");

        // avro part (deserializer)
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", SimpleAvroSchemaJava.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        properties.setProperty("specific.avro.reader", "true");

        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);


        FlinkKafkaConsumer<User> flinkkafkaconsumer = new FlinkKafkaConsumer<User>("inputGMFCSV4", new SimpleAvroSchemaFlink(), properties);

        flinkkafkaconsumer.setStartFromEarliest();
        DataStream<User> stream = env.addSource(flinkkafkaconsumer);


        System.out.println(stream.print());
        stream.print();

        FlinkKafkaProducer<User> flinkKafkaProducer = new FlinkKafkaProducer<>("outputGMFCSV4", new SimpleAvroSchemaFlink(), properties);
        stream.map(new MapFunction<User, User>() {
            @Override
            public User map(User userBehavior) throws Exception {
                userBehavior.setCSLVersion(userBehavior.getCSLVersion()+ " Name Updated");
                userBehavior.setCRNTI(userBehavior.getCRNTI()+ " Number Updated");
                return userBehavior;
            }
        }).addSink(flinkKafkaProducer);
        stream.print();
        env.execute("ReadFromKafka");
    }
}
