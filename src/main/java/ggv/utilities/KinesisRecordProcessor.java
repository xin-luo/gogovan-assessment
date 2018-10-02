package ggv.utilities;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;

import java.io.IOException;
import java.util.function.Function;

/**
 * Implementation of Kinesis's RecordProcessor which, along with the OrdersEventConsumerFactory, allow other classes
 * to easily attach functions to each orders_event Consumer and do separate processing on that event
 *
 * The reason for using this design pattern for injecting logic in the processor is that I think an app should have only
 * one Kinesis consumer per stream. It doesn't make sense resource-wise to have a separate consumer (worker) per class
 * as it's a waste of resources/processing power and makes it possible for metrics to be desynced.
 *
 * Another possibility is to have each record processor include a stream publisher that the factory controls, and
 * then each metric consumer can read from that stream (Flux) independently
 * @param <T>
 */
@Slf4j
public class KinesisRecordProcessor<T> implements IRecordProcessor {
    private final Class<T> clazz;
    private final Function<T, ?> function;
    private final Schema avroSchema;

    public KinesisRecordProcessor(Class<T> clazz, Function<T, ?> function) {
        this.clazz = clazz;
        this.function = function;

        avroSchema = ReflectData.get().getSchema(clazz);
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        log.info("Worker initialized: {}", initializationInput);
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        log.debug("Processing {} {} records...", processRecordsInput.getRecords().size(), clazz);

        processRecordsInput.getRecords().parallelStream().forEach(record -> {
            try {
                final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(record.getData().array(), null);
                final DatumReader<T> datumReader = new ReflectDatumReader<>(avroSchema);
                T orderEvent = datumReader.read(null, decoder);

                //run the registered function for the process which calls all the functions in the factory
                function.apply(orderEvent);
            } catch (IOException e) {
                log.warn("General IO Exception reading input: {}", e);
            }
        });

        log.debug("Processed {} {} records in a batch", processRecordsInput.getRecords().size(), clazz);
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        log.warn("Worker shutdown: {}", shutdownInput);
    }
}