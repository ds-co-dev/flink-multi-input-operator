package dev.ds_co.flink.streaming.api.operators.testing;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Out {
  private String id;
  private int sum;
}
