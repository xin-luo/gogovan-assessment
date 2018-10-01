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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

/**
 * Implementation of Kinesis's RecordProcessor which, along with the OrdersEventConsumerFactory, allow other classes
 * to easily attach functions to each orders_event Consumer and do separate processing on that event
 * @param <T>
 */
@Slf4j
public class KinesisRecordProcessor<T> implements IRecordProcessor {
    private Class<T> clazz;
    private final BlockingQueue<Function<T, Void>> functions;
    private final Schema avroSchema;

    public KinesisRecordProcessor(Class<T> clazz) {
        this.clazz = clazz;
        this.functions = new LinkedBlockingQueue<>();

        avroSchema = ReflectData.get().getSchema(clazz);
    }

    public void addFunction(Function<T, Void> function) {
        functions.add(function);
        log.info("Added function {} for {}. Now {} function registered.", function, clazz.getName(), functions.size());
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

                //run all the registered function for the process
                functions.forEach(function -> function.apply(orderEvent));
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