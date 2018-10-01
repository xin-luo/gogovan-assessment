package ggv.utilities.pojo;

import lombok.Value;

import java.util.Map;


@Value
public class TopRegionsMetric {
    private Map<String, Long> topShipFromRegions;
    private Map<String, Long> topShipToRegions;
}
