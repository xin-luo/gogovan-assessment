package ggv.utilities.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderAssignedEvent {
    long orderId;
    Instant assignmentTime;
    String driver;
}

