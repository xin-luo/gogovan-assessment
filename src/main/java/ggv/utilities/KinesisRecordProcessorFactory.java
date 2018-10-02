package ggv.utilities;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Lazily Generates Kinesis RecordProcessors for any event type and executes them for the first time.
 * Other classes can register call functions to attach to the event processors to avoid needing to make a new worker per client
 */
@Slf4j
@Service
@DependsOn("localKinesisExecutor")
public class KinesisRecordProcessorFactory {
    private final Map<Class<?>, BlockingQueue<Function<?, ?>>> functionsMap;
    private final KinesisUtilities utilities;
    private final ConfigurationProvider configuration;

    private final ExecutorService executor;

    public KinesisRecordProcessorFactory(ConfigurationProvider configuration, KinesisUtilities utilities) {
        executor = Executors.newCachedThreadPool();
        this.utilities = utilities;
        this.configuration = configuration;
        functionsMap = new ConcurrentHashMap<>();
    }

    //Creates a new RecordProcessor if one doesnt exist or returns the existing one.
    public <E> void createRecordProcessor(Class<E> type) {
        final String streamName = configuration.getEventClassToStreamMapping().get(type);
        try {
            final Worker worker = utilities.getKinesisWorker(streamName, this.getClass().getName(), () -> recordProcessorFactory(type));
            executor.execute(worker);
        } catch (InterruptedException e) {
            e.printStackTrace();
            log.error("Interrupted while attempting to create worker for RecordProcessor: {}", e);
        }
    }

    //Method for creating new RecordProcessors if we (or the worker) requests them
    //The typecasting is safe since we are creating the functions based on the type but the compiler doesnt know it
    //Should probably check how many of these gets spun up by the workers and make sure there arent any old
    //ones that arent being used and arent being garbage collected

    private <E> KinesisRecordProcessor<E> recordProcessorFactory(Class<E> type) {
        //when creating a new recordprocessor, it gets attached with a call that calls all the calls
        return new KinesisRecordProcessor<>(type, this::callProcessorFunction);

    }

    //adds a function call to all record processors for this type as well as to the list of functions
    //The typecasting is safe since we are creating the recordProcessors based on the type but the compiler doesnt know it
    public <E> void addFunction(Class<E> type, Function<E, ?> function) {
        AtomicBoolean isNew = new AtomicBoolean(false);
        functionsMap.computeIfAbsent(type, (key) -> {
            isNew.set(true);
            return new LinkedBlockingQueue<>();
        }).add(function);

        if (isNew.get()) {
            createRecordProcessor(type);
        }
    }

    public <E> Void callProcessorFunction(E element) {
        functionsMap.get(element.getClass()).forEach(function -> ((Function<E, ?>) function).apply(element));
        return null;
    }
}