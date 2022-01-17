
package gmf.utils;



import example.avro.User;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class SimpleAvroSchemaJava implements Serializer<User>, Deserializer<User> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }
    @Override
    public byte[] serialize(String s, User userBehavior) {
        SpecificDatumWriter<User> writer = new SpecificDatumWriter<User>(userBehavior.getSchema());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        try {
            writer.write(userBehavior, encoder);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return out.toByteArray();
    }

    @Override
    public void close() {

    }

    @Override
    public User deserialize(String s, byte[] bytes) {
        User userBehavior = new User();
        ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(bytes);
        SpecificDatumReader<User> stockSpecificDatumReader = new SpecificDatumReader<User>(userBehavior.getSchema());
        BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(arrayInputStream, null);
        try {
            userBehavior=stockSpecificDatumReader.read(null, binaryDecoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return userBehavior;
    }
}

