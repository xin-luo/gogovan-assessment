package ggv.metrics;

import ggv.utilities.ConfigurationProvider;
import ggv.utilities.KinesisRecordProcessor;
import ggv.utilities.KinesisRecordProcessorFactory;
import ggv.utilities.pojo.*;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MetricsTest {
    private final ConfigurationProvider configuration = Mockito.mock(ConfigurationProvider.class);
    private final KinesisRecordProcessorFactory processorFactory = Mockito.mock(KinesisRecordProcessorFactory.class);
    private final KinesisRecordProcessor processor = Mockito.mock(KinesisRecordProcessor.class);
    private final BlockingQueue<KinesisRecordProcessor> processorQueue = new LinkedBlockingQueue<>();

    private final String[] regions = new String[]{"a", "b", "c"};
    private final String driverName = "driver";
    private final String reasonCancelled = "no reason";

    private final int pickUpDelaySeconds1 = 1000;
    private final int assignedDelaySeconds1 = 2000;
    private final int completedDelaySeconds1 = 5000;
    private final double price1 = 123.45;

    private final int pickUpDelaySeconds2 = 2000;
    private final int assignedDelaySeconds2 = 3000;
    private final int cancelledDelaySeconds2 = 4000;
    private final double price2 = 456.78;

    private final String fromRegion = "from";
    private final String toRegion = "to";
    private final int numTopRegions = 10;

    Instant now = Instant.now();
    private final OrderCreatedEvent orderCreatedEvent1 = new OrderCreatedEvent(1, fromRegion, toRegion, now.plus(pickUpDelaySeconds1, ChronoUnit.SECONDS), now, price1);
    private final OrderAssignedEvent orderAssignedEvent1 = new OrderAssignedEvent(1, now.plus(assignedDelaySeconds1, ChronoUnit.SECONDS), driverName);
    private final OrderCompletedEvent orderCompletedEvent1 = new OrderCompletedEvent(1, now.plus(completedDelaySeconds1, ChronoUnit.SECONDS));

    private final OrderCreatedEvent orderCreatedEvent2 = new OrderCreatedEvent(2, fromRegion, toRegion, now.plus(pickUpDelaySeconds2, ChronoUnit.SECONDS), now, price2);
    private final OrderAssignedEvent orderAssignedEvent2 = new OrderAssignedEvent(2, now.plus(assignedDelaySeconds2, ChronoUnit.SECONDS), driverName);
    private final OrderCancelledEvent orderCancelledEvent2 = new OrderCancelledEvent(2, now.plus(cancelledDelaySeconds2, ChronoUnit.SECONDS), reasonCancelled);

    private AverageTimes averageTimes;
    private CurrentAmountsByStatus currentAmountsByStatus;
    private TopRegions topRegions;

    public MetricsTest() {
        processorQueue.add(processor);
        when(configuration.getTopShipRegionsNum()).thenReturn(numTopRegions);

        currentAmountsByStatus = new CurrentAmountsByStatus(processorFactory, configuration);
        topRegions = new TopRegions(processorFactory, configuration);

        doReturn(processorQueue).when(processorFactory).get(any());

        doAnswer((invocation) -> {
            invocation.callRealMethod();
            return null;
        }).when(processorFactory).addFunction(any(), any());
    }

    @Test
    public void averageTimesMetricWorks() {
        averageTimes = new AverageTimes(processorFactory, configuration);

        ArgumentCaptor<Class> clazz = ArgumentCaptor.forClass(Class.class);
        verify(processorFactory, times(4)).get(clazz.capture());

        List<Class> outputClasses = clazz.getAllValues();
        assertTrue(outputClasses.contains(OrderCreatedEvent.class));
        assertTrue(outputClasses.contains(OrderAssignedEvent.class));
        assertTrue(outputClasses.contains(OrderCompletedEvent.class));
        assertTrue(outputClasses.contains(OrderCancelledEvent.class));

        averageTimes.addOrderCreatedTimestamp(orderCreatedEvent1);
        averageTimes.setAssignedDurations(orderAssignedEvent1);
        averageTimes.setCompletedDurations(orderCompletedEvent1);

        AverageTimesMetric metric1 = averageTimes.emitMetric(0);

        assertEquals(completedDelaySeconds1, metric1.getAverageCompletionTime().getSeconds());
        assertEquals(assignedDelaySeconds1, metric1.getAverageResponseTime().getSeconds());

        averageTimes.addOrderCreatedTimestamp(orderCreatedEvent2);
        averageTimes.setAssignedDurations(orderAssignedEvent2);
        averageTimes.setCancellation(orderCancelledEvent2);

        AverageTimesMetric metric2 = averageTimes.emitMetric(0);

        assertEquals(completedDelaySeconds1, metric2.getAverageCompletionTime().getSeconds());
        assertEquals((assignedDelaySeconds1 + assignedDelaySeconds2)/2, metric2.getAverageResponseTime().getSeconds());
    }

    @Test
    public void currentAmountsByStatusWorks() {
        currentAmountsByStatus = new CurrentAmountsByStatus(processorFactory, configuration);

        ArgumentCaptor<Class> clazz = ArgumentCaptor.forClass(Class.class);
        verify(processorFactory, times(4)).get(clazz.capture());

        List<Class> outputClasses = clazz.getAllValues();
        assertTrue(outputClasses.contains(OrderCreatedEvent.class));
        assertTrue(outputClasses.contains(OrderAssignedEvent.class));
        assertTrue(outputClasses.contains(OrderCompletedEvent.class));
        assertTrue(outputClasses.contains(OrderCancelledEvent.class));

        CurrentAmountsByStatusMetric metric;

        currentAmountsByStatus.setOrderCreatedPrices(orderCreatedEvent1);
        metric = currentAmountsByStatus.emitMetric(0);
        assertEquals(price1, metric.getCreated().getCurrentAmount(), 0.01);
        assertEquals(1, metric.getCreated().getNumEvents());
        assertEquals(0, metric.getAssigned().getCurrentAmount(), 0.01);
        assertEquals(0, metric.getAssigned().getNumEvents());
        assertEquals(0, metric.getCompleted().getCurrentAmount(), 0.01);
        assertEquals(0, metric.getCompleted().getNumEvents());
        assertEquals(0, metric.getCancelled().getCurrentAmount(), 0.01);
        assertEquals(0, metric.getCancelled().getNumEvents());

        currentAmountsByStatus.transferToOrderAssignedPrices(orderAssignedEvent1);
        metric = currentAmountsByStatus.emitMetric(0);
        assertEquals(0, metric.getCreated().getCurrentAmount(), 0.01);
        assertEquals(0, metric.getCreated().getNumEvents());
        assertEquals(price1, metric.getAssigned().getCurrentAmount(), 0.01);
        assertEquals(1, metric.getAssigned().getNumEvents());
        assertEquals(0, metric.getCompleted().getCurrentAmount(), 0.01);
        assertEquals(0, metric.getCompleted().getNumEvents());
        assertEquals(0, metric.getCancelled().getCurrentAmount(), 0.01);
        assertEquals(0, metric.getCancelled().getNumEvents());

        currentAmountsByStatus.setOrderCreatedPrices(orderCreatedEvent2);
        metric = currentAmountsByStatus.emitMetric(0);
        assertEquals(price2, metric.getCreated().getCurrentAmount(), 0.01);
        assertEquals(1, metric.getCreated().getNumEvents());
        assertEquals(price1, metric.getAssigned().getCurrentAmount(), 0.01);
        assertEquals(1, metric.getAssigned().getNumEvents());
        assertEquals(0, metric.getCompleted().getCurrentAmount(), 0.01);
        assertEquals(0, metric.getCompleted().getNumEvents());
        assertEquals(0, metric.getCancelled().getCurrentAmount(), 0.01);
        assertEquals(0, metric.getCancelled().getNumEvents());

        currentAmountsByStatus.transferToOrderAssignedPrices(orderAssignedEvent2);
        metric = currentAmountsByStatus.emitMetric(0);
        assertEquals(0, metric.getCreated().getCurrentAmount(), 0.01);
        assertEquals(0, metric.getCreated().getNumEvents());
        assertEquals(price1 + price2, metric.getAssigned().getCurrentAmount(), 0.01);
        assertEquals(2, metric.getAssigned().getNumEvents());
        assertEquals(0, metric.getCompleted().getCurrentAmount(), 0.01);
        assertEquals(0, metric.getCompleted().getNumEvents());
        assertEquals(0, metric.getCancelled().getCurrentAmount(), 0.01);
        assertEquals(0, metric.getCancelled().getNumEvents());

        currentAmountsByStatus.transferToOrderCompletedPrices(orderCompletedEvent1);
        metric = currentAmountsByStatus.emitMetric(0);
        assertEquals(0, metric.getCreated().getCurrentAmount(), 0.01);
        assertEquals(0, metric.getCreated().getNumEvents());
        assertEquals(price2, metric.getAssigned().getCurrentAmount(), 0.01);
        assertEquals(1, metric.getAssigned().getNumEvents());
        assertEquals(price1, metric.getCompleted().getCurrentAmount(), 0.01);
        assertEquals(1, metric.getCompleted().getNumEvents());
        assertEquals(0, metric.getCancelled().getCurrentAmount(), 0.01);
        assertEquals(0, metric.getCancelled().getNumEvents());

        currentAmountsByStatus.transferToOrderCancelledPrices(orderCancelledEvent2);
        metric = currentAmountsByStatus.emitMetric(0);
        assertEquals(0, metric.getCreated().getCurrentAmount(), 0.01);
        assertEquals(0, metric.getCreated().getNumEvents());
        assertEquals(0, metric.getAssigned().getCurrentAmount(), 0.01);
        assertEquals(0, metric.getAssigned().getNumEvents());
        assertEquals(price1, metric.getCompleted().getCurrentAmount(), 0.01);
        assertEquals(1, metric.getCompleted().getNumEvents());
        assertEquals(price2, metric.getCancelled().getCurrentAmount(), 0.01);
        assertEquals(1, metric.getCancelled().getNumEvents());
    }

    @Test
    public void topRegionsWorks() {
        topRegions = new TopRegions(processorFactory, configuration);

        ArgumentCaptor<Class> clazz = ArgumentCaptor.forClass(Class.class);
        verify(processorFactory, times(1)).get(clazz.capture());

        assertEquals(OrderCreatedEvent.class, clazz.getValue());

        TopRegionsMetric metric;

        topRegions.computeTopShipRegions(orderCreatedEvent1);
        metric = topRegions.emitMetric(0);
        assertEquals(1, metric.getTopShipFromRegions().size());
        assertEquals(1, metric.getTopShipToRegions().size());
        assertTrue(metric.getTopShipFromRegions().containsKey(fromRegion));
        assertTrue(metric.getTopShipToRegions().containsKey(toRegion));
        assertEquals(1, metric.getTopShipFromRegions().get(fromRegion).intValue());
        assertEquals(1, metric.getTopShipToRegions().get(toRegion).intValue());

        topRegions.computeTopShipRegions(orderCreatedEvent2);
        metric = topRegions.emitMetric(0);
        assertEquals(1, metric.getTopShipFromRegions().size());
        assertEquals(1, metric.getTopShipToRegions().size());
        assertTrue(metric.getTopShipFromRegions().containsKey(fromRegion));
        assertTrue(metric.getTopShipToRegions().containsKey(toRegion));
        assertEquals(2, metric.getTopShipFromRegions().get(fromRegion).intValue());
        assertEquals(2, metric.getTopShipToRegions().get(toRegion).intValue());

        topRegions = new TopRegions(processorFactory, configuration);

        int max = 25;
        for (int i = 0; i < max; i++) {
            for (int j = 0; j < max; j++) {
                topRegions.computeTopShipRegions(new OrderCreatedEvent(i*max+j, fromRegion + (i * j), toRegion + (i + j), now, now, price1));
            }
        }

        metric = topRegions.emitMetric(0);
        assertEquals(numTopRegions, metric.getTopShipFromRegions().size());
        assertEquals(numTopRegions, metric.getTopShipToRegions().size());
        assertTrue(metric.getTopShipFromRegions().containsKey(fromRegion + "0"));
        assertTrue(metric.getTopShipToRegions().containsKey(toRegion + (max - 1)));
        assertEquals(max*2-1, metric.getTopShipFromRegions().get(fromRegion + "0").intValue());
        assertEquals(max, metric.getTopShipToRegions().get(toRegion + (max - 1)).intValue());
    }
}
