package dev.ds_co.flink.streaming.api.operators.examples.processingtime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Ride-side facts for a {@code rideId} (input 1 of {@link RideFareBundleOperator}). */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RideEvent {
  private long rideId;
  private int passengers;
}
