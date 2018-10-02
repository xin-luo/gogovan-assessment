package ggv.metrics;

import ggv.utilities.ConfigurationProvider;
import ggv.utilities.KinesisRecordProcessorFactory;
import ggv.utilities.pojo.OrderCreatedEvent;
import ggv.utilities.pojo.TopRegionsMetric;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

import static java.util.stream.Collectors.toMap;

/**
 * Gets the top x Regions where orders are shipped from and shipped to
 */
@Slf4j
@Service
public class TopRegions {
    private final ConcurrentMap<String, LongAdder> shipFromRegions;
    private final ConcurrentMap<String, LongAdder> shipToRegions;

    private final int topShipRegionsNum;
    private final Flux<TopRegionsMetric> topRegionsMetricsProcessor;

    public TopRegions(KinesisRecordProcessorFactory kinesisRecordProcessorFactory, ConfigurationProvider configuration) {
        shipFromRegions = new ConcurrentHashMap<>();
        shipToRegions = new ConcurrentHashMap<>();
        topShipRegionsNum = configuration.getTopShipRegionsNum();

        kinesisRecordProcessorFactory.addFunction(OrderCreatedEvent.class, this::computeTopShipRegions);

        topRegionsMetricsProcessor = Flux.interval(Duration.ofSeconds(configuration.getEmitFrequencySeconds())).map(this::emitMetric);
    }

    protected Void computeTopShipRegions(OrderCreatedEvent orderCreatedEvent) {
        shipFromRegions.computeIfAbsent(orderCreatedEvent.getFromRegion(), k -> new LongAdder()).increment();
        shipToRegions.computeIfAbsent(orderCreatedEvent.getToRegion(), k -> new LongAdder()).increment();
        return null;
    }

    protected TopRegionsMetric emitMetric(long emitter) {
        if (shipFromRegions.size() > 0) {
            final Map<String, Long> topShipFromRegions = shipFromRegions
                    .entrySet()
                    .stream()
                    .sorted((a, b) -> Long.compare(b.getValue().longValue(), a.getValue().longValue()))
                    .limit(topShipRegionsNum)
                    .collect(toMap(e -> e.getKey(), e -> e.getValue().longValue(), (e1, e2) -> e1, LinkedHashMap::new));

            final Map<String, Long> topShipToRegions = shipToRegions
                    .entrySet()
                    .stream()
                    .sorted((a, b) -> Long.compare(b.getValue().longValue(), a.getValue().longValue()))
                    .limit(topShipRegionsNum)
                    .collect(toMap(e -> e.getKey(), e -> e.getValue().longValue(), (e1, e2) -> e1, LinkedHashMap::new));

            return new TopRegionsMetric(topShipFromRegions, topShipToRegions);
        }
        else {
            return new TopRegionsMetric(new HashMap<>(), new HashMap<>());
        }
    }

    public Flux topRegionsMetricsProcessor() {
        return topRegionsMetricsProcessor;
    }
}
