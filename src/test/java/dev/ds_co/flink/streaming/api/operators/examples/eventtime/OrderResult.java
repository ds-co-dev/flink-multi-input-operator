package dev.ds_co.flink.streaming.api.operators.examples.eventtime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderResult {
  public enum Status {
    COMPLETED,
    TIMED_OUT
  }

  private String orderId;
  private Status status;
  private boolean hasPayment;
  private boolean hasShipment;
}
