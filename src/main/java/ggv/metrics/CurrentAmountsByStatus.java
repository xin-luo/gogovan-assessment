package ggv.metrics;

import ggv.utilities.ConfigurationProvider;
import ggv.utilities.KinesisRecordProcessorFactory;
import ggv.utilities.pojo.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

/**
 * Get the current state of all orders and the amount of money that exists in that state. If an order transitions from state1 to state2,
 * its value will no longer count towards the total for state1.
 *
 * Since the subsequent events do not persist the price, we have to keep track of the current state of the system by monitoring the stream
 * This implementation keeps a Map for each imtermediate state, storing the orderId and price for each order and shuffling them around.
 * For the order_completed event, we can simply track the total amount (and optionally, total # orders) as that is the last step.
 */
@Slf4j
@Service
public class CurrentAmountsByStatus {
    private final ConcurrentMap<Long, Double> ordersCreatedPrices;
    private final ConcurrentMap<Long, Double> ordersAssignedPrices;
    private final ConcurrentMap<Long, Instant> ordersTTL;
    private final DoubleAdder ordersCompletedPrices;
    private final LongAdder ordersCompletedCount;
    private final DoubleAdder ordersCancelledPrices;
    private final LongAdder ordersCancelledCount;

    private final Flux<CurrentAmountsByStatusMetric> currentAmountsProcessor;

    public CurrentAmountsByStatus(KinesisRecordProcessorFactory kinesisRecordProcessorFactory, ConfigurationProvider configuration) {
        ordersCreatedPrices = new ConcurrentHashMap<>();
        ordersAssignedPrices = new ConcurrentHashMap<>();
        ordersTTL = new ConcurrentHashMap<>();
        ordersCompletedPrices = new DoubleAdder();
        ordersCompletedCount = new LongAdder();
        ordersCancelledPrices = new DoubleAdder();
        ordersCancelledCount = new LongAdder();

        kinesisRecordProcessorFactory.addFunction(OrderCreatedEvent.class, this::setOrderCreatedPrices);
        kinesisRecordProcessorFactory.addFunction(OrderAssignedEvent.class, this::transferToOrderAssignedPrices);
        kinesisRecordProcessorFactory.addFunction(OrderCompletedEvent.class, this::transferToOrderCompletedPrices);
        kinesisRecordProcessorFactory.addFunction(OrderCancelledEvent.class, this::transferToOrderCancelledPrices);

        currentAmountsProcessor = Flux.interval(Duration.ofSeconds(configuration.getEmitFrequencySeconds())).map(this::emitMetric);
    }

    protected CurrentAmountsByStatusMetric emitMetric(long emitter) {
        final EventAmountAndCount created = new EventAmountAndCount(ordersCreatedPrices.size(), ordersCreatedPrices.values().stream().mapToDouble(Number::doubleValue).sum());
        final EventAmountAndCount assigned = new EventAmountAndCount(ordersAssignedPrices.size(), ordersAssignedPrices.values().stream().mapToDouble(Number::doubleValue).sum());
        final EventAmountAndCount completed = new EventAmountAndCount(ordersCompletedCount.longValue(), ordersCompletedPrices.doubleValue());
        final EventAmountAndCount cancelled = new EventAmountAndCount(ordersCancelledCount.longValue(), ordersCancelledPrices.doubleValue());

        return new CurrentAmountsByStatusMetric(created, assigned, completed, cancelled);
    }

    protected Void setOrderCreatedPrices(OrderCreatedEvent orderCreatedEvent) {
        long orderId = orderCreatedEvent.getOrderId();
        ordersCreatedPrices.put(orderId, orderCreatedEvent.getPrice());
        ordersTTL.put(orderId, orderCreatedEvent.getCreatedTime());
        return null;
    }

    protected Void transferToOrderAssignedPrices(OrderAssignedEvent orderAssignedEvent) {
        final long orderId = orderAssignedEvent.getOrderId();
        final Double price = ordersCreatedPrices.remove(orderId);
        if (price != null) {
            ordersAssignedPrices.put(orderId, price);
        }
        else {
            log.warn("Could not find order_created with id {} in memory", orderId);
        }
        return null;
    }

    protected Void transferToOrderCompletedPrices(OrderCompletedEvent orderCompletedEvent) {
        final long orderId = orderCompletedEvent.getOrderId();
        final Double price = ordersAssignedPrices.remove(orderId);
        if (price != null) {
            ordersCompletedPrices.add(price);
            ordersCompletedCount.increment();
        }
        else {
            log.warn("Could not find order_assigned event with id {} in memory", orderId);
        }
        return null;
    }

    protected Void transferToOrderCancelledPrices(OrderCancelledEvent orderCancelledEvent) {
        final long orderId = orderCancelledEvent.getOrderId();
        Double price = ordersAssignedPrices.remove(orderId);
        if (price != null || (price = ordersCreatedPrices.remove(orderId)) != null) {
            ordersCancelledPrices.add(price);
            ordersCancelledCount.increment();
        }
        else {
            log.warn("Could not find order_assigned or order_created event with id {} in memory", orderId);
        }
        return null;
    }

    public Flux currentValuesMetricsProcessor() {
        return currentAmountsProcessor;
    }
}
