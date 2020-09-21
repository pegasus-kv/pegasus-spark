package com.xiaomi.infra.pegasus.spark;

import com.revinate.guava.util.concurrent.RateLimiter;
import java.io.Serializable;

public class FlowController {

  public static class RateLimiterConfig implements Serializable {
    private long megabytes;
    private long qps;
    private double burstFactor;

    public RateLimiterConfig() {
      this.megabytes = Long.MAX_VALUE;
      this.qps = Long.MAX_VALUE;
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

  public FlowController(int partitionCount, long qps, long megabytes, double factor) {
    this.qpsLimiter = RateLimiter.create(1.0 * qps / partitionCount, qps * factor / partitionCount);
    this.bytesLimiter =
        RateLimiter.create(
            1.0 * (megabytes << 20) / partitionCount, (megabytes << 20) * factor / partitionCount);
  }

  public void acquireBytes(int bytes) {
    bytesLimiter.acquire(bytes);
  }

  public void acquireQPS() {
    qpsLimiter.acquire();
  }
}
