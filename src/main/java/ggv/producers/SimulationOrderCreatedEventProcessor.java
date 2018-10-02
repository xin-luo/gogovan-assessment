package ggv.producers;

import ggv.utilities.*;
import ggv.utilities.pojo.OrderAssignedEvent;
import ggv.utilities.pojo.OrderCancelledEvent;
import ggv.utilities.pojo.OrderCreatedEvent;
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
 * Reads the stream of order_created events and has a configurable chance of generating an order_assigned event sometime in the future
 * or an order_cancelled event immediately
 */
@Slf4j
@Service
public class SimulationOrderCreatedEventProcessor {
    private final double pctOrdersAssigned;
    private final int assignAdvanceMaxSeconds;
    private final int assignDelayMinSeconds;
    private final int assignDelayMaxSeconds;
    private final KinesisEventProducer<OrderAssignedEvent> assignedEventProducer;
    private final KinesisEventProducer<OrderCancelledEvent> cancelledEventProducer;
    private final AvailableDrivers drivers;
    private final ScheduledExecutorService executor;

    private static final String DRIVER_CANCELLATION_REASON = "No drivers available.";
    private static final String PCT_CANCELLATION_REASON = "Cancelled by customer.";

    public SimulationOrderCreatedEventProcessor(ConfigurationProvider configuration,
                                                KinesisRecordProcessorFactory kinesisRecordProcessorFactory,
                                                KinesisEventProducerFactory kinesisEventProducerFactory,
                                                AvailableDrivers drivers) {

        pctOrdersAssigned = configuration.getPctOrdersAssigned();
        assignAdvanceMaxSeconds = configuration.getAssignAdvanceMaxSeconds();
        assignDelayMinSeconds = configuration.getAssignDelayMinSeconds();
        assignDelayMaxSeconds = configuration.getAssignDelayMaxSeconds();

        this.assignedEventProducer = kinesisEventProducerFactory.get(OrderAssignedEvent.class);
        this.cancelledEventProducer = kinesisEventProducerFactory.get(OrderCancelledEvent.class);
        this.drivers = drivers;

        executor = Executors.newSingleThreadScheduledExecutor();

        kinesisRecordProcessorFactory.addFunction(OrderCreatedEvent.class, this::processOrderCreatedEvent);
    }

    /**
     * A Function that gets hooked up to the orders_created Consumer for processing that generates the subsequent orders_assigned
     * @param orderCreatedEvent
     * @return
     */
    protected Void processOrderCreatedEvent(OrderCreatedEvent orderCreatedEvent) {
        if (ThreadLocalRandom.current().nextDouble() * 100 < pctOrdersAssigned) {
            Instant now = Instant.now();

            final String driver = drivers.pollDriver(2);
            if (driver != null) {
                OrderAssignedEvent orderAssignedEvent = new OrderAssignedEvent(
                        orderCreatedEvent.getOrderId(),
                        getRandomAssignmentTime(orderCreatedEvent.getPickupTime(), now),
                        driver);


                log.trace("Scheduling order_assigned event for {} milliseconds from now", Duration.between(now, orderAssignedEvent.getAssignmentTime()).toMillis());
                //schedule the order_assigned event based on the assignment time

                executor.schedule(new GenerateOrderAssignedEventRunner(orderAssignedEvent),
                        Math.max(Duration.between(now, orderAssignedEvent.getAssignmentTime()).toMillis(), 0),
                        TimeUnit.MILLISECONDS);
            }
            else {
                log.debug("Rejecting assignment of this order due to lack of drivers.");
                cancelledEventProducer.sendEvent(new OrderCancelledEvent(orderCreatedEvent.getOrderId(), Instant.now(), DRIVER_CANCELLATION_REASON));
            }
        }
        else {
            cancelledEventProducer.sendEvent(new OrderCancelledEvent(orderCreatedEvent.getOrderId(), Instant.now(), PCT_CANCELLATION_REASON));
            log.debug("Rejecting assignment of this order due to set percentage.");
        }
        return null;
    }

    /**
     * A Runnable that gets scheduled for sending the order_assigned event to the producer after a given delay
     */
    private class GenerateOrderAssignedEventRunner implements Runnable {
        final OrderAssignedEvent orderAssignedEvent;

        public GenerateOrderAssignedEventRunner(OrderAssignedEvent orderAssignedEvent) {
            this.orderAssignedEvent = orderAssignedEvent;
        }

        @Override
        public void run() {
            assignedEventProducer.sendEvent(orderAssignedEvent);
            log.trace("Sent event: {}", orderAssignedEvent);
        }
    }

    /**
     * Determines based on configured values a random delay within bounds between receiving an order_created event
     * and sending the order_assigned event. Also depends on the event's pickup_time
     * @param pickupTime
     * @param now
     * @return
     */
    private Instant getRandomAssignmentTime(Instant pickupTime, Instant now) {
        if (pickupTime.isAfter(now.plus(assignAdvanceMaxSeconds, ChronoUnit.SECONDS))) {
            //order is in the mid-distant future, we can assign it up to assignAdvanceMaxSeconds before the pickup time
            return pickupTime.minus((long) (assignAdvanceMaxSeconds * ThreadLocalRandom.current().nextDouble()), ChronoUnit.SECONDS);
        }
        else {
            //order is in the near future, system will assign it ASAP, with an given delay from assignDelayMinSeconds assignDelayMaxSeconds
            return now.plus((long) (assignDelayMinSeconds + ((assignDelayMaxSeconds - assignDelayMinSeconds) * ThreadLocalRandom.current().nextDouble())), ChronoUnit.SECONDS);
        }
    }
}
