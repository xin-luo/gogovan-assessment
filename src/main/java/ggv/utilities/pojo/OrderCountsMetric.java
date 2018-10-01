package ggv.utilities.pojo;

import lombok.Value;

@Value
public class OrderCountsMetric {
    long ordersCreated;
    long ordersAssigned;
    long ordersCompleted;
    long ordersCancelled;
    long numDrivers;
}
