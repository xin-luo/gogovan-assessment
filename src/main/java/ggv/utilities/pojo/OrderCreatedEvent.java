package ggv.utilities.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderCreatedEvent {
    long orderId;
    String fromRegion;
    String toRegion;
    Instant pickupTime;
    Instant createdTime;
    double price;
}
