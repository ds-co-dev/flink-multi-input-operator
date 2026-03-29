package dev.ds_co.flink.streaming.api.operators.examples.eventtime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Shipment {
  private String orderId;
  private long eventTime;
}
