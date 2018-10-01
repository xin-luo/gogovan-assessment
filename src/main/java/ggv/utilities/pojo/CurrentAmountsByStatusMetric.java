package ggv.utilities.pojo;

import lombok.Value;

@Value
public class CurrentAmountsByStatusMetric {
    EventAmountAndCount created;
    EventAmountAndCount assigned;
    EventAmountAndCount completed;
    EventAmountAndCount cancelled;
}
