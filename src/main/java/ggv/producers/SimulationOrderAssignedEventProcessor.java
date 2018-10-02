package ggv.producers;

import ggv.utilities.*;
import ggv.utilities.pojo.OrderAssignedEvent;
import ggv.utilities.pojo.OrderCancelledEvent;
import ggv.utilities.pojo.OrderCompletedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Reads the stream of order_assigned events and has a configurable chance of generating an order_created event sometime in the future
 * or an order_cancelled event immediately
 */
@Slf4j
@Service
public class SimulationOrderAssignedEventProcessor {
    private final double pctOrdersCompleted;
    private final int completeDelayMinSeconds;
    private final int completeDelayMaxSeconds;
    private final KinesisEventProducer<OrderCompletedEvent> completedEventProducer;
    private final KinesisEventProducer<OrderCancelledEvent> cancelledEventProducer;
    private final AvailableDrivers drivers;
    private final ScheduledExecutorService executor;

    private static final String[] CANCELLATION_REASONS = new String[]{"Accident during journey", "Customer cancelled trip", "Lost connection"};

    public SimulationOrderAssignedEventProcessor(ConfigurationProvider configuration,
                                                 KinesisRecordProcessorFactory kinesisRecordProcessorFactory,
                                                 KinesisEventProducerFactory kinesisEventProducerFactory,
                                                 AvailableDrivers drivers)  {
        pctOrdersCompleted = configuration.getPctOrdersCompleted();
        completeDelayMinSeconds = configuration.getCompleteDelayMinSeconds();
        completeDelayMaxSeconds = configuration.getCompleteDelayMaxSeconds();

        this.completedEventProducer = kinesisEventProducerFactory.get(OrderCompletedEvent.class);
        this.cancelledEventProducer = kinesisEventProducerFactory.get(OrderCancelledEvent.class);
        this.drivers = drivers;

        executor = Executors.newSingleThreadScheduledExecutor();

        kinesisRecordProcessorFactory.addFunction(OrderAssignedEvent.class, this::processOrderAssignedEvent);
    }

    /**
     * A Function that gets hooked up to the orders_assigned Consumer for processing that generates the subsequent orders_completed event
     * @param orderAssignedEvent
     * @return
     */
    protected Void processOrderAssignedEvent(OrderAssignedEvent orderAssignedEvent) {
        if (ThreadLocalRandom.current().nextDouble() * 100 < pctOrdersCompleted) {

            OrderCompletedEvent orderCompletedEvent = new OrderCompletedEvent(orderAssignedEvent.getOrderId(),
                    getRandomCompletedTime(orderAssignedEvent.getAssignmentTime()));

            //schedule sending the order_completed event based on the assignment time
            executor.schedule(new GenerateOrderCompletedEventRunner(orderCompletedEvent, orderAssignedEvent.getDriver()),
                    Math.max(Duration.between(Instant.now(), orderCompletedEvent.getCompletedTime()).toMillis(), 0),
                    TimeUnit.MILLISECONDS);
        }
        else {
            cancelledEventProducer.sendEvent(new OrderCancelledEvent(orderAssignedEvent.getOrderId(), Instant.now(), CANCELLATION_REASONS[ThreadLocalRandom.current().nextInt(CANCELLATION_REASONS.length)]));
            log.debug("Rejecting completion of this order due to set percentage.");
            //return driver to the pool
            drivers.addDriver(orderAssignedEvent.getDriver());
        }
        return null;
    }

    /**
     * A Runnable that gets scheduled for sending the order_completed event to the producer after a given delay
     */
    private class GenerateOrderCompletedEventRunner implements Runnable {
        final OrderCompletedEvent orderCompletedEvent;
        final String driver;

        public GenerateOrderCompletedEventRunner(OrderCompletedEvent orderCompletedEvent, String driver) {
            this.orderCompletedEvent = orderCompletedEvent;
            this.driver = driver;
        }

        @Override
        public void run() {
            completedEventProducer.sendEvent(orderCompletedEvent);
            drivers.addDriver(driver);
            log.trace("Sent event: {}", orderCompletedEvent);
        }
    }

    /**
     * Determines based on configured values a random delay within bounds between receiving an order_assigned event
     * and sending the order_completed event.
     * @return
     */
    private Instant getRandomCompletedTime(Instant assignedTime) {
        return assignedTime.plus((long) (completeDelayMinSeconds + (completeDelayMaxSeconds - completeDelayMinSeconds) * ThreadLocalRandom.current().nextDouble()), ChronoUnit.SECONDS);
    }
}
