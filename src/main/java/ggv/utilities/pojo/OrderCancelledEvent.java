package ggv.utilities.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderCancelledEvent {
    long orderId;
    Instant cancelledTime;
    String reason;
}