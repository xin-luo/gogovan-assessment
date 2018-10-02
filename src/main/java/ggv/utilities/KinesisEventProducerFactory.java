package ggv.utilities;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * This is a factory for conveniently generating a KinesisEventProducer for each of the streams we need
 * so that the generators dont have to deal with injecting KinesisUtilities or mapping the class to the stream name
 */
@Slf4j
@Service
@DependsOn("localKinesisExecutor")
public class KinesisEventProducerFactory {
    private final Map<Class<?>, String> eventClassToStream;
    private final KinesisUtilities utilities;

    public KinesisEventProducerFactory(KinesisUtilities utilities, ConfigurationProvider configuration) {
        this.utilities = utilities;
        eventClassToStream = configuration.getEventClassToStreamMapping();
    }

    public <E> KinesisEventProducer<E> get(Class<E> clazz) {
        return new KinesisEventProducer<>(eventClassToStream.get(clazz), utilities, clazz);
    }
}