package ggv.metrics;

import ggv.utilities.pojo.AverageTimesMetric;
import ggv.utilities.pojo.CurrentAmountsByStatusMetric;
import ggv.utilities.pojo.OrderCountsMetric;
import ggv.utilities.pojo.TopRegionsMetric;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@CrossOrigin
@RestController
public class MetricsAPIController {
    private EventCounts eventCounts;
    private AverageTimes averageTimes;
    private CurrentAmountsByStatus currentAmounts;
    private TopRegions topRegions;

    public MetricsAPIController(EventCounts eventCounts, AverageTimes averageTimes, CurrentAmountsByStatus currentAmounts, TopRegions topRegions) {
        this.eventCounts = eventCounts;
        this.averageTimes = averageTimes;
        this.currentAmounts = currentAmounts;
        this.topRegions = topRegions;
    }

    @RequestMapping("/endpoints")
    public String endpoints() {
        return "<a href='/counts'>Counts</a><br />" +
                "<a href='/averageTimes'>Average Wait Times</a><br />" +
                "<a href='/currentAmounts'>Current Pricing Amounts by Status</a><br />" +
                "<a href='/topRegions'>Top Regions Shipping From and To</a><br />";
    }

    @GetMapping(value="/counts", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<OrderCountsMetric> getEventCounts(){
        return eventCounts.countMetricsProcessor();
    }

    @GetMapping(value="/averageTimes", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<AverageTimesMetric> getAverageTimes(){
        return averageTimes.avgTimesMetricsProcessor();
    }

    @GetMapping(value="/currentAmounts", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<CurrentAmountsByStatusMetric> getCurrentAmounts(){ return currentAmounts.currentValuesMetricsProcessor(); }

    @GetMapping(value="/topRegions", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<TopRegionsMetric> getTopRegions(){
        return topRegions.topRegionsMetricsProcessor();
    }

}