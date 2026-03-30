package dev.ds_co.flink.streaming.api.operators.examples.processingtime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Fare / payment line for a {@code rideId} (input 2 of {@link RideFareBundleOperator}). */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FareEvent {
  private long rideId;
  private float totalFare;
}
