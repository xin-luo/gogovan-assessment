package ggv.utilities;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

/**
 * Lazily Generates Kinesis RecordProcessors for any event type and executes them for the first time.
 * Other classes can register call functions to attach to the event processor to avoid needing to make a new consumer per processor
 *
 * There's some nasty type inference going on because factory generates untyped processors based on the given class
 * so it can support any event without explicitly creating them one by one, at the cost of potentially unsafe calls
 * as the event type is not enforced
 */
@Slf4j
@Service
@DependsOn("localKinesisExecutor")
public class KinesisRecordProcessorFactory {
    private final Map<Class<?>, KinesisRecordProcessor<?>> recordProcessorMap;
    private final KinesisUtilities utilities;
    private final ConfigurationProvider configuration;

    private final ExecutorService executor;

    public KinesisRecordProcessorFactory(ConfigurationProvider configuration, KinesisUtilities utilities) {
        executor = Executors.newCachedThreadPool();
        this.utilities = utilities;
        this.configuration = configuration;
        recordProcessorMap = new ConcurrentHashMap<>();
    }

    //Creates a new RecordProcessor if one doesnt exist or returns the existing one
    public KinesisRecordProcessor<?> get(Class<?> type) {
        return recordProcessorMap.computeIfAbsent(type, (alsoType) -> {
            final KinesisRecordProcessor<?> newRecordProcessor = new KinesisRecordProcessor<>(alsoType);
            final String streamName = configuration.getEventClassToStreamMapping().get(alsoType);
            final Worker worker;
            try {
                worker = utilities.getKinesisWorker(streamName, this.getClass().getName(), () -> newRecordProcessor);
                executor.execute(worker);
            } catch (InterruptedException e) {
                e.printStackTrace();
                log.error("Interrupted while attemping to create worker for RecordProcessor: {}", e);
                return null;
            }

            return newRecordProcessor;
        });
    }

    //adds a function call to a record processor
    public void addFunction(Class<?> type, Function function) {
        get(type).addFunction(function);
    }
}
