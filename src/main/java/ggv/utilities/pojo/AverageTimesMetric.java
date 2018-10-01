package ggv.utilities.pojo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Value;

import java.time.Duration;

@Value
public class AverageTimesMetric {

    @JsonFormat(shape = JsonFormat.Shape.NUMBER)
    private Duration averageResponseTime;

    @JsonFormat(shape = JsonFormat.Shape.NUMBER)
    private Duration averageCompletionTime;
}
