package com.xiaomi.infra.pegasus.spark;

import com.revinate.guava.util.concurrent.RateLimiter;
import java.io.Serializable;

public class FlowController {

  public static class RateLimiterConfig implements Serializable {
    private long megabytes;
    private long qps;
    private double burstFactor;

    public RateLimiterConfig() {
      this.megabytes = 0;
      this.qps = 0;
      this.burstFactor = 1;
    }

    public RateLimiterConfig setMegabytes(long megabytes) {
      this.megabytes = megabytes;
      return this;
    }

    public RateLimiterConfig setQps(long qps) {
      this.qps = qps;
      return this;
    }

    public RateLimiterConfig setBurstFactor(double burstFactor) {
      this.burstFactor = burstFactor;
      return this;
    }

    public long getMegabytes() {
      return megabytes;
    }

    public long getQps() {
      return qps;
    }

    public double getBurstFactor() {
      return burstFactor;
    }
  }

  private RateLimiter bytesLimiter;
  private RateLimiter qpsLimiter;

  private int partitionCount;
  private double burstFactor;

  public FlowController(int partitionCount, double burstFactor) {
    this.partitionCount = partitionCount;
    this.burstFactor = burstFactor;
  }

  public FlowController withMBytesLimiter(long megabytes) {
    if (megabytes <= 0) {
      return this;
    }

    this.bytesLimiter =
        RateLimiter.create(
            1.0 * (megabytes << 20) / partitionCount,
            (megabytes << 20) * burstFactor / partitionCount);
    return this;
  }

  public FlowController withQPSLimiter(long qps) {
    if (qps <= 0) {
      return this;
    }

    this.qpsLimiter =
        RateLimiter.create(1.0 * qps / partitionCount, qps * burstFactor / partitionCount);
    return this;
  }

  public void acquireBytes(int bytes) {
    if (bytesLimiter == null) {
      return;
    }
    bytesLimiter.acquire(bytes);
  }

  public void acquireQPS() {
    if (qpsLimiter == null) {
      return;
    }
    qpsLimiter.acquire();
  }
}
