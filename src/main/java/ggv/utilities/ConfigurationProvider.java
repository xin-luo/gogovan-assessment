package ggv.utilities;

import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import ggv.utilities.pojo.OrderAssignedEvent;
import ggv.utilities.pojo.OrderCancelledEvent;
import ggv.utilities.pojo.OrderCompletedEvent;
import ggv.utilities.pojo.OrderCreatedEvent;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.reflect.ReflectData;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Reads in the configuration settings from application.properties (or env variables) for setting up the app
 */
@Slf4j
@Getter
@Configuration
public class ConfigurationProvider implements WebMvcConfigurer {
    @Value("${producer.numOrdersPerSecond}")
    private int numOrdersPerSecond;

    @Value("${producer.pctOrdersAssigned}")
    private double pctOrdersAssigned;

    @Value("${producer.pctOrdersCompleted}")
    private double pctOrdersCompleted;

    @Value("${producer.pickupTimeMaxDelaySeconds}")
    private int pickupTimeMaxDelaySeconds;

    @Value("${producer.assignAdvanceMaxSeconds}")
    private int assignAdvanceMaxSeconds;

    @Value("${producer.assignDelayMinSeconds}")
    private int assignDelayMinSeconds;

    @Value("${producer.assignDelayMaxSeconds}")
    private int assignDelayMaxSeconds;

    @Value("${producer.completeDelayMinSeconds}")
    private int completeDelayMinSeconds;

    @Value("${producer.completeDelayMaxSeconds}")
    private int completeDelayMaxSeconds;

    @Value("${producer.minPrice}")
    private double minPrice;

    @Value("${producer.maxPrice}")
    private double maxPrice;

    @Value("${producer.regions}")
    private String[] regions;

    @Value("${producer.drivers}")
    private String[] drivers;

    @Value("${producer.numDrivers}")
    private int numDrivers;

    @Value("${aws.region}")
    private String awsRegion;

    @Value("${aws.isLocal}")
    private boolean awsIsLocal;

    @Value("${aws.metricsLevel}")
    private String metricsLevel;

    @Value("${kinesis.endpoint}")
    private String kinesisEndpoint;

    @Value("${kinesis.port}")
    private int kinesisPort;

    @Value("${kinesis.shardCount}")
    private int shardCount;

    @Value("${dynamodb.endpoint}")
    private String dynamodbEndpoint;

    @Value("${dynamodb.port}")
    private int dynamodbPort;

    @Value("${stream.orderCreated}")
    private String orderCreatedStreamName;

    @Value("${stream.orderAssigned}")
    private String orderAssignedStreamName;

    @Value("${stream.orderCompleted}")
    private String orderCompletedStreamName;

    @Value("${stream.orderCancelled}")
    private String orderCancelledStreamName;

    @Value("${ggv.metrics.emitFrequencySeconds}")
    private int emitFrequencySeconds;

    @Value("${ggv.metrics.topShipRegionsNum}")
    private int topShipRegionsNum;

    @Value("${web.numThreads}")
    private int webNumThreads;

    //Disable sending ggv.metrics to Cloudwatch
    @Bean
    public IMetricsFactory getMetricsFactory() {
        return new NullMetricsFactory();
    }

    //Need this specific ObjectMapper that supports serdes on Instant class
    @Bean
    public ObjectMapper getObjectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule()).configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    private final Map<Class<?>, String> eventClassToStreamMapping;


    public ConfigurationProvider() {
        //One time set Avro to use Instant converter
        ReflectData.get().addLogicalTypeConversion(new AvroInstantConverter());

        eventClassToStreamMapping = new HashMap<>();
    }

    /**
     * This is used by the RestAPIController to divide up the work when serving Fluxes to various client consumers
     * @param configurer
     */
    @Override
    public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
        configurer.setTaskExecutor(new ConcurrentTaskExecutor(Executors.newFixedThreadPool(webNumThreads)));
    }

    @PostConstruct
    public void init() {
        //Define which stream names go into which event class pojo for convience
        eventClassToStreamMapping.put(OrderCreatedEvent.class, getOrderCreatedStreamName());
        eventClassToStreamMapping.put(OrderAssignedEvent.class, getOrderAssignedStreamName());
        eventClassToStreamMapping.put(OrderCompletedEvent.class, getOrderCompletedStreamName());
        eventClassToStreamMapping.put(OrderCancelledEvent.class, getOrderCancelledStreamName());
    }
}
