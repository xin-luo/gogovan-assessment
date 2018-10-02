package ggv.producers;

import ggv.utilities.ConfigurationProvider;
import ggv.utilities.KinesisEventProducer;
import ggv.utilities.KinesisEventProducerFactory;
import ggv.utilities.pojo.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Service for generating simulated order_created events periodically
 */
@Slf4j
@Service
public class SimulationOrderCreatedEventGenerator {
    private final int ordersPerSecond;
    private final double minPrice;
    private final double maxPrice;
    private final double pickupTimeMaxDelaySeconds;
    private final String[] regions;
    private long eventId;

    private final KinesisEventProducer<OrderCreatedEvent> eventProducer;
    private final ScheduledExecutorService executor;
    private final GenerateOrderCreatedEventRunner eventRunner;
    private final Random random;

    private final String outputStream;

    public SimulationOrderCreatedEventGenerator(ConfigurationProvider configuration,
                                                KinesisEventProducerFactory kinesisEventProducerFactory) {
        ordersPerSecond = configuration.getNumOrdersPerSecond();
        minPrice = configuration.getMinPrice();
        maxPrice = configuration.getMaxPrice();
        pickupTimeMaxDelaySeconds = configuration.getPickupTimeMaxDelaySeconds();
        regions = configuration.getRegions();
        outputStream = configuration.getOrderCreatedStreamName();
        eventId = 0;

        this.eventProducer = kinesisEventProducerFactory.get(OrderCreatedEvent.class);
        random = new Random();

        executor = Executors.newSingleThreadScheduledExecutor();
        eventRunner = new GenerateOrderCreatedEventRunner();
    }

    /**
     * Only want to start emitting results once everything else is completely initiated
     */
    @PostConstruct
    public void init() {
        executor.scheduleAtFixedRate(eventRunner, 0, Math.round(1000000D/ordersPerSecond), TimeUnit.MICROSECONDS);
    }

    /**
     * Runnable that gets scheduled at a fixed rate to roughly ensure we are getting the expected number of events per second
     */
    private class GenerateOrderCreatedEventRunner implements Runnable {
        @Override
        public void run() {
            Instant now = Instant.now();
            OrderCreatedEvent orderCreatedEvent = new OrderCreatedEvent(++eventId, getRandomRegion(), getRandomRegion(), getRandomPickupTime(now), now, getRandomPrice());
            eventProducer.sendEvent(orderCreatedEvent);
            log.trace("Sent {} event: {}", outputStream, orderCreatedEvent);
        }
    }

    /**
     * Randomly picks a region for populating the order_created event
     * @return
     */
    private String getRandomRegion() {
        return regions[(int) (regions.length * random.nextDouble())];
    }

    /**
     * Randomly selects a pickupTime some time in the near future
     * @return
     */
    private Instant getRandomPickupTime(Instant now) {return now.plus((long) (pickupTimeMaxDelaySeconds * random.nextDouble()), ChronoUnit.SECONDS); }

    /**
     * Randomly determines the price of the fare for this trip
     * @return
     */
    private double getRandomPrice() {
        return minPrice + (maxPrice - minPrice) * random.nextDouble();
    }
}