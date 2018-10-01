package ggv.metrics;

import ggv.utilities.ConfigurationProvider;
import ggv.utilities.KinesisRecordProcessorFactory;
import ggv.utilities.pojo.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Calculates the average time for each order spent between the start and the assignment, and the start until the completion.
 * Excludes orders that get cancelled or never complete (configurable as a percentage)
 *
 * Since the subsequent events do not persist the original event dates, we have to keep track of the current state of the system by monitoring the stream
 * This implementation keeps a Map for the Created state, storing the orderId and timestamp for each order until their trip is complete.
 * For the order_assigned and order_completed event, we can simply track the total time taken and total # orders to compute the average.
 */
@Slf4j
@Service
public class AverageTimes {
    private final ConcurrentMap<Long, Instant> ordersCreatedTimestamps;

    private final LongAdder ordersAssignedDurations;
    private final LongAdder ordersAssignedCount;
    private final LongAdder ordersCompletedDurations;
    private final LongAdder ordersCompletedCount;
    private final LongAdder ordersCancelledDurations;
    private final LongAdder ordersCancelledCount;

    private final Flux<AverageTimesMetric> avgTimesMetricsProcessor;

    public AverageTimes(KinesisRecordProcessorFactory kinesisRecordProcessorFactory, ConfigurationProvider configuration) {
        ordersCreatedTimestamps = new ConcurrentHashMap<>();
        ordersAssignedDurations = new LongAdder();
        ordersAssignedCount = new LongAdder();
        ordersCompletedDurations = new LongAdder();
        ordersCompletedCount = new LongAdder();
        ordersCancelledDurations = new LongAdder();
        ordersCancelledCount = new LongAdder();

        kinesisRecordProcessorFactory.addFunction(OrderCreatedEvent.class, (input) -> addOrderCreatedTimestamp((OrderCreatedEvent) input));
        kinesisRecordProcessorFactory.addFunction(OrderAssignedEvent.class, (input) -> setAssignedDurations((OrderAssignedEvent) input));
        kinesisRecordProcessorFactory.addFunction(OrderCompletedEvent.class, (input) -> setCompletedDurations((OrderCompletedEvent) input));
        kinesisRecordProcessorFactory.addFunction(OrderCancelledEvent.class, (input) -> setCancellation((OrderCancelledEvent) input));

        avgTimesMetricsProcessor = Flux.interval(Duration.ofSeconds(configuration.getEmitFrequencySeconds())).map(this::emitMetric);
    }

    protected AverageTimesMetric emitMetric(long emitter) {
        final Duration responseTimes;
        if (ordersAssignedCount.longValue() > 0) {
            responseTimes = Duration.of(ordersAssignedDurations.longValue() / ordersAssignedCount.longValue(), ChronoUnit.MILLIS);
        }
        else {
            responseTimes = null;
        }

        final Duration completionTimes;
        if (ordersCompletedCount.longValue() > 0) {
            completionTimes = Duration.of(ordersCompletedDurations.longValue() / ordersCompletedCount.longValue(), ChronoUnit.MILLIS);
        }
        else {
            completionTimes = null;
        }

        AverageTimesMetric metric = new AverageTimesMetric(responseTimes, completionTimes);
        log.debug("Average Time Taken: {}", metric);
        return metric;
    }

    protected Void addOrderCreatedTimestamp(OrderCreatedEvent orderCreatedEvent) {
        ordersCreatedTimestamps.put(orderCreatedEvent.getOrderId(), orderCreatedEvent.getCreatedTime());
        return null;
    }

    protected Void setAssignedDurations(OrderAssignedEvent orderAssignedEvent) {
        final long orderId = orderAssignedEvent.getOrderId();
        final Instant originalCreatedTime = ordersCreatedTimestamps.get(orderId);
        if (originalCreatedTime != null) {
            ordersAssignedDurations.add(Duration.between(originalCreatedTime, orderAssignedEvent.getAssignmentTime()).toMillis());
            ordersAssignedCount.increment();
        }
        else {
            log.warn("Received assigned event but could not find order_created with id {} in memory", orderId);
        }
        return null;
    }

    protected Void setCompletedDurations(OrderCompletedEvent orderCompletedEvent) {
        final long orderId = orderCompletedEvent.getOrderId();
        final Instant originalCreatedTime = ordersCreatedTimestamps.remove(orderId);
        if (originalCreatedTime != null) {
            ordersCompletedDurations.add(Duration.between(originalCreatedTime, orderCompletedEvent.getCompletedTime()).toMillis());
            ordersCompletedCount.increment();
        }
        else {
            log.warn("Received completed event but could not find order_created with id {} in memory", orderId);
        }
        return null;
    }

    protected Void setCancellation(OrderCancelledEvent orderCancelledEvent) {
        final long orderId = orderCancelledEvent.getOrderId();
        final Instant originalCreatedTime = ordersCreatedTimestamps.remove(orderId);
        if (originalCreatedTime != null) {
            ordersCancelledDurations.add(Duration.between(originalCreatedTime, orderCancelledEvent.getCancelledTime()).toMillis());
            ordersCancelledCount.increment();
        }
        else {
            log.warn("Received cancellation event but could not find order_created with id {} in memory", orderId);
        }
        return null;
    }

    public Flux avgTimesMetricsProcessor() {
        return avgTimesMetricsProcessor;
    }
}
