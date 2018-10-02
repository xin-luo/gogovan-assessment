package ggv.producers;

import ggv.utilities.*;
import ggv.utilities.pojo.OrderAssignedEvent;
import ggv.utilities.pojo.OrderCancelledEvent;
import ggv.utilities.pojo.OrderCompletedEvent;
import ggv.utilities.pojo.OrderCreatedEvent;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class OrderGeneratedTest {
    private ConfigurationProvider configuration = Mockito.mock(ConfigurationProvider.class);
    private KinesisRecordProcessorFactory processorFactory = Mockito.mock(KinesisRecordProcessorFactory.class);
    private KinesisEventProducerFactory producerFactory = Mockito.mock(KinesisEventProducerFactory.class);
    private KinesisEventProducer producer = Mockito.mock(KinesisEventProducer.class);
    private BlockingQueue<KinesisRecordProcessor<?>> processorQueue = new LinkedBlockingQueue<>();
    private KinesisRecordProcessor processor = Mockito.mock(KinesisRecordProcessor.class);
    private AvailableDrivers drivers = Mockito.mock(AvailableDrivers.class);

    private SimulationOrderCreatedEventGenerator orderCreatedGenerator;
    private SimulationOrderCreatedEventProcessor orderCreatedProcessor;
    private SimulationOrderAssignedEventProcessor orderAssignedProcessor;

    private final int numOrdersPerSecond = 100;
    private final String[] regions = new String[]{"a", "b", "c"};
    private final String driverName = "driver";
    private final String reasonCancelled = "no reason";
    private final int pickUpTimeMaxDelaySeconds = 10;
    private final int assignAdvanceMaxSeconds = 0;
    private final int assignDelayMinSeconds = 0;
    private final int assignDelayMaxSeconds = 0;
    private final double pctOrdersAssigned = 50;

    private final OrderCreatedEvent orderCreatedEvent = new OrderCreatedEvent(1, "from", "to", Instant.now(), Instant.now(), 55.5);
    private final OrderAssignedEvent orderAssignedEvent = new OrderAssignedEvent(1, Instant.now(), driverName);
    private final OrderCompletedEvent orderCompletedEvent = new OrderCompletedEvent(1, Instant.now());
    private final OrderCancelledEvent orderCancelledEvent = new OrderCancelledEvent(1, Instant.now(), reasonCancelled);

    public OrderGeneratedTest() {
        processorQueue.add(processor);

        when(configuration.getNumOrdersPerSecond()).thenReturn(numOrdersPerSecond);
        when(configuration.getRegions()).thenReturn(regions);
        when(configuration.getPickupTimeMaxDelaySeconds()).thenReturn(pickUpTimeMaxDelaySeconds);
        when(configuration.getAssignAdvanceMaxSeconds()).thenReturn(assignAdvanceMaxSeconds);
        when(configuration.getAssignDelayMinSeconds()).thenReturn(assignDelayMinSeconds);
        when(configuration.getAssignAdvanceMaxSeconds()).thenReturn(assignDelayMaxSeconds);
        when(configuration.getPctOrdersAssigned()).thenReturn(pctOrdersAssigned);
        when(producerFactory.get(any())).thenReturn(producer);

        doReturn(processorQueue).when(processorFactory).get(any());

        doAnswer((invocation) -> {
            processorFactory.get(invocation.getArgument(0));
            processor.addFunction(invocation.getArgument(1));
            return null;
        }).when(processorFactory).addFunction(any(), any());

        when(drivers.getNumDrivers()).thenReturn(999L);
        doReturn(driverName).when(drivers).pollDriver(anyInt());
    }

    @Test
    public void orderCreatedGeneratorWorks() {
        Instant start = Instant.now();
        orderCreatedGenerator = new SimulationOrderCreatedEventGenerator(configuration, producerFactory);

        orderCreatedGenerator.init();

        ArgumentCaptor<Class> clazz = ArgumentCaptor.forClass(Class.class);
        verify(producerFactory, times(1)).get(clazz.capture());

        assertEquals(OrderCreatedEvent.class, clazz.getValue());

        ArgumentCaptor<OrderCreatedEvent> argument = ArgumentCaptor.forClass(OrderCreatedEvent.class);

        verify(producer, timeout(1000).atLeast(numOrdersPerSecond-2)).sendEvent(argument.capture());
        verify(producer, atMost(numOrdersPerSecond+2)).sendEvent(any());

        List<OrderCreatedEvent> output = argument.getAllValues();

        int i = 0;
        for (OrderCreatedEvent event : output) {
            assertEquals(++i, event.getOrderId());
            assertTrue(Arrays.asList(regions).contains(event.getFromRegion()));
            assertTrue(Arrays.asList(regions).contains(event.getToRegion()));
            assertFalse(Duration.between(start, event.getCreatedTime()).isNegative());
            assertFalse(Duration.between(event.getCreatedTime(), event.getPickupTime()).isNegative());
            assertTrue(event.getCreatedTime().plus((long) Math.ceil(pickUpTimeMaxDelaySeconds*1000), ChronoUnit.MILLIS).isAfter(event.getPickupTime()));
        }
    }

    @Test
    public void orderCreatedProcessorWorks() {
        Instant start = Instant.now();
        orderCreatedProcessor = new SimulationOrderCreatedEventProcessor(configuration, processorFactory, producerFactory, drivers);

        ArgumentCaptor<Class> clazz = ArgumentCaptor.forClass(Class.class);
        verify(producerFactory, times(2)).get(clazz.capture());
        List<Class> outputClasses = clazz.getAllValues();
        assertTrue(outputClasses.contains(OrderAssignedEvent.class));
        assertTrue(outputClasses.contains(OrderCancelledEvent.class));

        clazz = ArgumentCaptor.forClass(Class.class);
        verify(processorFactory, times(1)).get(clazz.capture());
        outputClasses = clazz.getAllValues();
        assertTrue(outputClasses.contains(OrderCreatedEvent.class));

        verify(processor, times(1)).addFunction(any());

        for (int i = 1; i <= 1000; i++) {
            OrderCreatedEvent newOrderCreatedEvent = new OrderCreatedEvent(i, "from", "to", Instant.now(), Instant.now(), 55.5);
            orderCreatedProcessor.processOrderCreatedEvent(newOrderCreatedEvent);
        }

        ArgumentCaptor<?> argument = ArgumentCaptor.forClass(OrderAssignedEvent.class);
        verify(producer, timeout(100).times(1000)).sendEvent(argument.capture());

        for (Object event : argument.getAllValues()) {

            if (event instanceof OrderAssignedEvent) {
                OrderAssignedEvent orderAssignedEvent = (OrderAssignedEvent) event;
                assertTrue(orderAssignedEvent.getOrderId() > 0 && orderAssignedEvent.getOrderId() <= 1000);
                assertFalse(Duration.between(start, orderAssignedEvent.getAssignmentTime()).isNegative());
                assertEquals(driverName, orderAssignedEvent.getDriver());
            }
            else if (event instanceof  OrderCancelledEvent) {
                OrderCancelledEvent orderCancelledEvent = (OrderCancelledEvent) event;
                assertTrue(orderCancelledEvent.getOrderId() > 0 && orderCancelledEvent.getOrderId() <= 1000);
            }
        }
    }

    @Test
    public void orderAssignedProcessorWorks() {
        Instant start = Instant.now();
        orderAssignedProcessor = new SimulationOrderAssignedEventProcessor(configuration, processorFactory, producerFactory, drivers);

        ArgumentCaptor<Class> clazz = ArgumentCaptor.forClass(Class.class);
        verify(producerFactory, times(2)).get(clazz.capture());

        List<Class> outputClasses = clazz.getAllValues();
        assertTrue(outputClasses.contains(OrderCompletedEvent.class));
        assertTrue(outputClasses.contains(OrderCancelledEvent.class));

        clazz = ArgumentCaptor.forClass(Class.class);
        verify(processorFactory, times(1)).get(clazz.capture());
        outputClasses = clazz.getAllValues();
        assertTrue(outputClasses.contains(OrderAssignedEvent.class));

        verify(processor, times(1)).addFunction(any());

        for (int i = 1; i <= 1000; i++) {
            OrderAssignedEvent orderAssignedEvent = new OrderAssignedEvent(i, Instant.now(), driverName);
            orderAssignedProcessor.processOrderAssignedEvent(orderAssignedEvent);
        }

        ArgumentCaptor<?> argument = ArgumentCaptor.forClass(OrderAssignedEvent.class);
        verify(producer, timeout(100).times(1000)).sendEvent(argument.capture());

        for (Object event : argument.getAllValues()) {

            if (event instanceof OrderCompletedEvent) {
                OrderCompletedEvent orderCompletedEvent = (OrderCompletedEvent) event;
                assertTrue(orderCompletedEvent.getOrderId() > 0 && orderCompletedEvent.getOrderId() <= 1000);
                assertFalse(Duration.between(start, orderCompletedEvent.getCompletedTime()).isNegative());
            }
            else if (event instanceof  OrderCancelledEvent) {
                OrderCancelledEvent orderCancelledEvent = (OrderCancelledEvent) event;
                assertTrue(orderCancelledEvent.getOrderId() > 0 && orderCancelledEvent.getOrderId() <= 1000);
            }
        }
    }
}
