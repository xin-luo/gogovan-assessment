package ggv.utilities;

import ggv.utilities.pojo.OrderAssignedEvent;
import ggv.utilities.pojo.OrderCompletedEvent;
import ggv.utilities.pojo.OrderCreatedEvent;
import ggv.utilities.serde.AvroInstantConverter;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class AvroSerdeTest {
    private OrderCreatedEvent orderCreatedSource1;
    private OrderAssignedEvent orderAssignedSource1;
    private OrderCompletedEvent orderCompletedSource1;

    private OrderCreatedEvent orderCreatedSource2;
    private OrderAssignedEvent orderAssignedSource2;
    private OrderCompletedEvent orderCompletedSource2;

    private ReflectDatumWriter<OrderCreatedEvent> orderCreatedWriter;
    private ReflectDatumWriter<OrderAssignedEvent> orderAssignedWriter;
    private ReflectDatumWriter<OrderCompletedEvent> orderCompletedWriter;

    private ReflectDatumReader<OrderCreatedEvent> orderCreatedReader;
    private ReflectDatumReader<OrderAssignedEvent> orderAssignedReader;
    private ReflectDatumReader<OrderCompletedEvent> orderCompletedReader;

    Schema orderCreatedSchema;
    Schema orderAssignedSchema;
    Schema orderCompletedSchema;


    private BinaryEncoder binaryEncoder;
    private BinaryDecoder binaryDecoder;

    private Random random;

    @Before
    public void init() {
        orderCreatedSource1 = new OrderCreatedEvent(1, "start", "end", Instant.now(), Instant.now(), 15.35);
        orderAssignedSource1 = new OrderAssignedEvent(1, Instant.now(), "driver");
        orderCompletedSource1 = new OrderCompletedEvent(1, Instant.now());

        orderCreatedSource2 = new OrderCreatedEvent(2, "start", "end", Instant.now(), Instant.now(), 100);
        orderAssignedSource2 = new OrderAssignedEvent(2, Instant.now(), "driver");
        orderCompletedSource2 = new OrderCompletedEvent(2, Instant.now());

        ReflectData.get().addLogicalTypeConversion(new AvroInstantConverter());

        orderCreatedSchema = ReflectData.get().getSchema(OrderCreatedEvent.class);
        orderAssignedSchema = ReflectData.get().getSchema(OrderAssignedEvent.class);
        orderCompletedSchema = ReflectData.get().getSchema(OrderCompletedEvent.class);

        orderCreatedWriter = new ReflectDatumWriter<>(orderCreatedSchema);
        orderAssignedWriter = new ReflectDatumWriter<>(orderAssignedSchema);
        orderCompletedWriter = new ReflectDatumWriter<>(orderCompletedSchema);

        orderCreatedReader = new ReflectDatumReader<>(orderCreatedSchema);
        orderAssignedReader = new ReflectDatumReader<>(orderAssignedSchema);
        orderCompletedReader = new ReflectDatumReader<>(orderCompletedSchema);

        random  = new Random();
    }

    @Test
    public void canSerializeCreated() throws IOException {
        ByteArrayOutputStream orderCreatedByteStream = new ByteArrayOutputStream();

        binaryEncoder = EncoderFactory.get().binaryEncoder(orderCreatedByteStream, binaryEncoder);
        orderCreatedWriter.write(orderCreatedSource1, binaryEncoder);
        binaryEncoder.flush();

        binaryDecoder = DecoderFactory.get().binaryDecoder(orderCreatedByteStream.toByteArray(), binaryDecoder);
        OrderCreatedEvent orderCreatedOutput1 = orderCreatedReader.read(null, binaryDecoder);

        assertEquals(orderCreatedSource1, orderCreatedOutput1);
    }

    @Test
    public void canSerializeAssigned() throws IOException {
        ByteArrayOutputStream orderAssignedByteStream = new ByteArrayOutputStream();

        BinaryEncoder orderAssignedBinaryEncoder = EncoderFactory.get().binaryEncoder(orderAssignedByteStream, null);
        orderAssignedWriter.write(orderAssignedSource1, orderAssignedBinaryEncoder);
        orderAssignedBinaryEncoder.flush();

        binaryDecoder = DecoderFactory.get().binaryDecoder(orderAssignedByteStream.toByteArray(), binaryDecoder);
        OrderAssignedEvent orderAssignedOutput1 = orderAssignedReader.read(null, binaryDecoder);

        assertEquals(orderAssignedSource1, orderAssignedOutput1);
    }

    @Test
    public void canSerializeCompleted() throws IOException {
        ByteArrayOutputStream orderCompletedByteStream = new ByteArrayOutputStream();

        BinaryEncoder orderCompletedBinaryEncoder = EncoderFactory.get().binaryEncoder(orderCompletedByteStream, null);
        orderCompletedWriter.write(orderCompletedSource1, orderCompletedBinaryEncoder);
        orderCompletedBinaryEncoder.flush();

        binaryDecoder = DecoderFactory.get().binaryDecoder(orderCompletedByteStream.toByteArray(), binaryDecoder);
        OrderCompletedEvent orderCompletedOutput1 = orderCompletedReader.read(null, binaryDecoder);

        assertEquals(orderCompletedSource1, orderCompletedOutput1);
    }

    //Apparently need to recreate a new reader/writer/encoder/decoder every time...
    @Test
    public void canSerializeInParallel() {

        List<OrderCreatedEvent> orderCreatedEvents = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            orderCreatedEvents.add(new OrderCreatedEvent(i, "start" + i, "end" + i, Instant.now(), Instant.now(), random.nextDouble()));
        }

        orderCreatedEvents.parallelStream().forEach(orderCreatedEvent -> {
            ByteArrayOutputStream orderCreatedByteStream = new ByteArrayOutputStream();

            try {
                BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(orderCreatedByteStream, null);
                DatumWriter datumWriter = new ReflectDatumWriter<>(orderCreatedSchema);
                datumWriter.write(orderCreatedEvent, binaryEncoder);
                binaryEncoder.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }

            OrderCreatedEvent orderCreatedOutput1 = null;
            try {
                BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(orderCreatedByteStream.toByteArray(), null);
                ReflectDatumReader<OrderCreatedEvent> datumReader = new ReflectDatumReader<>(orderCreatedSchema);
                orderCreatedOutput1 = datumReader.read(null, binaryDecoder);
            } catch (IOException e) {
                e.printStackTrace();
            }

            assertEquals(orderCreatedEvent, orderCreatedOutput1);
        });


    }


}
