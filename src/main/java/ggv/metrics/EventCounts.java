package ggv.metrics;

import ggv.utilities.AvailableDrivers;
import ggv.utilities.ConfigurationProvider;
import ggv.utilities.KinesisRecordProcessorFactory;
import ggv.utilities.pojo.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.atomic.LongAdder;

/**
 * Simple metric generator for seeing the total number of events that have entered each stream.
 * I was using this for debugging purposes
 */
@Slf4j
@Service
public class EventCounts {
    private final LongAdder ordersCreatedCount;
    private final LongAdder ordersAssignedCount;
    private final LongAdder ordersCompletedCount;
    private final LongAdder ordersCancelledCount;

    private final Flux<OrderCountsMetric> countsMetricsProcessor;

    public EventCounts(KinesisRecordProcessorFactory kinesisRecordProcessorFactory, AvailableDrivers drivers, ConfigurationProvider configuration) {
        ordersCreatedCount = new LongAdder();
        ordersAssignedCount = new LongAdder();
        ordersCompletedCount = new LongAdder();
        ordersCancelledCount = new LongAdder();

        kinesisRecordProcessorFactory.addFunction(OrderCreatedEvent.class, this::incrementCreatedCount);
        kinesisRecordProcessorFactory.addFunction(OrderAssignedEvent.class, this::incrementAssignedCount);
        kinesisRecordProcessorFactory.addFunction(OrderCompletedEvent.class, this::incrementCompletedCount);
        kinesisRecordProcessorFactory.addFunction(OrderCancelledEvent.class, this::incrementCancelledCount);

        //Emits a message every x seconds only when the flux is subscribed to
        countsMetricsProcessor = Flux.interval(Duration.ofSeconds(configuration.getEmitFrequencySeconds())).map(emitter -> {
            log.debug("Created: {}, Assigned: {}, Completed: {}, Cancelled: {} Drivers: {}", ordersCreatedCount.longValue(),
                    ordersAssignedCount.longValue(),
                    ordersCompletedCount.longValue(),
                    ordersCompletedCount.longValue(),
                    drivers.getNumDrivers());
            return new OrderCountsMetric(ordersCreatedCount.longValue(),
                    ordersAssignedCount.longValue(),
                    ordersCompletedCount.longValue(),
                    ordersCancelledCount.longValue(),
                    drivers.getNumDrivers());
        });
    }

    protected Void incrementCreatedCount(OrderCreatedEvent orderCreatedEvent) {
        ordersCreatedCount.increment();
        return null;
    }

    protected Void incrementAssignedCount(OrderAssignedEvent orderAssignedEvent) {
        ordersAssignedCount.increment();
        return null;
    }

    protected Void incrementCompletedCount(OrderCompletedEvent orderCompletedEvent) {
        ordersCompletedCount.increment();
        return null;
    }

    protected Void incrementCancelledCount(OrderCancelledEvent orderCancelledEvent) {
        ordersCancelledCount.increment();
        return null;
    }

    public Flux countMetricsProcessor() {
        return countsMetricsProcessor;
    }
}
