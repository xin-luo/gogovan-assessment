package ggv.utilities;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * Lazily Generates Kinesis RecordProcessors for any event type and executes them for the first time.
 * Other classes can register call functions to attach to the event processors to avoid needing to make a new worker per client
 */
@Slf4j
@Service
@DependsOn("localKinesisExecutor")
public class KinesisRecordProcessorFactory {
    private final Map<Class<?>, BlockingQueue<KinesisRecordProcessor<?>>> recordProcessorMap;
    private final Map<Class<?>, BlockingQueue<Function<?, ?>>> functionsMap;
    private final KinesisUtilities utilities;
    private final ConfigurationProvider configuration;

    private final ExecutorService executor;

    public KinesisRecordProcessorFactory(ConfigurationProvider configuration, KinesisUtilities utilities) {
        executor = Executors.newCachedThreadPool();
        this.utilities = utilities;
        this.configuration = configuration;
        recordProcessorMap = new ConcurrentHashMap<>();
        functionsMap = new ConcurrentHashMap<>();
    }

    //Creates a new RecordProcessor if one doesnt exist or returns the existing one.
    public <E> BlockingQueue<? extends KinesisRecordProcessor<?>> get(Class<E> type) {
        return recordProcessorMap.computeIfAbsent(type, (key) -> {
            final BlockingQueue<KinesisRecordProcessor<?>> recordProcessorQueue = new LinkedBlockingQueue<>();
            final String streamName = configuration.getEventClassToStreamMapping().get(type);
            try {
                final Worker worker = utilities.getKinesisWorker(streamName, this.getClass().getName(), () -> recordProcessorFactory(type, recordProcessorQueue));
                executor.execute(worker);
            } catch (InterruptedException e) {
                e.printStackTrace();
                log.error("Interrupted while attempting to create worker for RecordProcessor: {}", e);
                return null;
            }
            return recordProcessorQueue;
        });
    }

    //Method for creating new RecordProcessors if we (or the worker) requests them
    //The typecasting is safe since we are creating the functions based on the type but the compiler doesnt know it
    //Should probably check how many of these gets spun up by the workers and make sure there arent any old
    //ones that arent being used and arent being garbage collected
    private <E> KinesisRecordProcessor<E> recordProcessorFactory(Class<E> type, BlockingQueue<KinesisRecordProcessor<?>> recordProcessorQueue) {
        final KinesisRecordProcessor<E> recordProcessor = new KinesisRecordProcessor<>(type);
        //when creating a new recordprocessor, make sure it gets attached with all known existing function calls
        functionsMap.get(type).forEach(function -> recordProcessor.addFunction((Function<E, ?>) function));

        recordProcessorQueue.add(recordProcessor);
        return recordProcessor;
    }

    //adds a function call to all record processors for this type as well as to the list of functions
    //The typecasting is safe since we are creating the recordProcessors based on the type but the compiler doesnt know it
    public <E> void addFunction(Class<E> type, Function<E, ?> function) {
        final BlockingQueue<Function<?, ?>> functionsQueue = functionsMap.computeIfAbsent(type, (key) -> new LinkedBlockingQueue<>());
        functionsQueue.add(function);

        get(type).forEach(recordProcessor -> ((KinesisRecordProcessor<E>) recordProcessor).addFunction(function));
    }
}