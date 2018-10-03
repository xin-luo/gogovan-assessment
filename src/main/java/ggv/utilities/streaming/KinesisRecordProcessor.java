package ggv.utilities.streaming;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import ggv.utilities.serde.ByteDeserializer;
import lombok.extern.slf4j.Slf4j;

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
    private final ByteDeserializer<T> deserializer;


    public KinesisRecordProcessor(Class<T> clazz, Function<T, ?> function, ByteDeserializer<T> deserializer) {
        this.clazz = clazz;
        this.function = function;
        this.deserializer = deserializer;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        log.info("Worker initialized: {}", initializationInput);
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        log.debug("Processing {} {} records...", processRecordsInput.getRecords().size(), clazz);

        processRecordsInput.getRecords().parallelStream().forEach(record -> {
            T orderEvent = deserializer.deserialize(record.getData().array());

            //run the registered function for the process which calls all the functions in the factory
            function.apply(orderEvent);
        });

        log.debug("Processed {} {} records in a batch", processRecordsInput.getRecords().size(), clazz);
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        log.warn("Worker shutdown: {}", shutdownInput);
    }
}