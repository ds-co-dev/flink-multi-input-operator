package dev.ds_co.flink.streaming.api.operators.examples.processingtime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Assembled snapshot for one {@code rideId}: latest ride, fare, and tip seen before the quiet
 * period ended. Fields may be null if that leg never arrived.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RideFareBundle {
  private long rideId;
  private RideEvent ride;
  private FareEvent fare;
  private TipEvent tip;
}
