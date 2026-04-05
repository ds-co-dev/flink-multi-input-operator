package dev.ds_co.flink.streaming.api.operators.testing;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Out implements Serializable {
  private String id;
  private int sum;
}
