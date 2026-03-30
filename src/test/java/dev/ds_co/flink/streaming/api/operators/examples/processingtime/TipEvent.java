package dev.ds_co.flink.streaming.api.operators.examples.processingtime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Tip for a {@code rideId} (input 3 of {@link RideFareBundleOperator}). */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TipEvent {
  private long rideId;
  private float tip;
}
