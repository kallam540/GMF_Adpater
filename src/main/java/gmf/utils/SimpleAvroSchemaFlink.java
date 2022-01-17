package gmf.utils;


import example.avro.User;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class SimpleAvroSchemaFlink implements DeserializationSchema<User>, SerializationSchema<User> {


    @Override
    public byte[] serialize(User userBehavior) {
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
    public TypeInformation<User> getProducedType() {
      return TypeInformation.of(User.class);
    }

    @Override
    public User deserialize(byte[] bytes) throws IOException {
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

    @Override
    public boolean isEndOfStream(User userBehavior) {
        return false;
    }
}
