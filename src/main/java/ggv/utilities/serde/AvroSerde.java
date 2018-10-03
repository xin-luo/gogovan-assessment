package ggv.utilities.serde;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Slf4j
public class AvroSerde<T> implements ByteDeserializer<T>, ByteSerializer<T> {
    private final Schema avroSchema;

    public AvroSerde(Class<T> clazz) {
        avroSchema = ReflectData.get().getSchema(clazz);
    }

    public T deserialize(byte[] input) {
        final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(input, null);
        final DatumReader<T> datumReader = new ReflectDatumReader<>(avroSchema);
        try {
            return datumReader.read(null, decoder);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Failed to deserialize input {}", new String(input));
            return null;
        }
    }

    public byte[] serialize(T input) {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        final DatumWriter<T> writer = new ReflectDatumWriter<>(avroSchema);
        try {
            writer.write(input, binaryEncoder);
            binaryEncoder.flush();
            byteArrayOutputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
            log.error("IO Exception while attempting to serialize object {}: {}", input, e);
        }
        return byteArrayOutputStream.toByteArray();
    }
}
