package com.xiaomi.infra.pegasus.spark.utils;

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
      this.burstFactor = 1.5;
    }

    /**
     * set the throughput MB/s, exceed the value will be blocked, default 0 means no limit
     *
     * @param megabytes MB/s
     * @return this
     */
    public RateLimiterConfig setMegabytes(long megabytes) {
      assert megabytes >= 0 : "megabytes >= 0";
      this.megabytes = megabytes;
      return this;
    }

    /**
     * set the qps, exceed the value will be blocked, default 0 means no limit
     *
     * @param qps
     * @return
     */
    public RateLimiterConfig setQps(long qps) {
      assert qps >= 0 : "qps >= 0";
      this.qps = qps;
      return this;
    }

    /**
     * set the burst factor, the burstRate = baseRate * burstFactor, default is 1.5
     *
     * @param burstFactor
     * @return
     */
    public RateLimiterConfig setBurstFactor(double burstFactor) {
      assert burstFactor >= 1 : "burstFactor >=1";
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

  public FlowController(int partitionCount, RateLimiterConfig config) {
    if (config.megabytes > 0) {
      this.bytesLimiter =
          RateLimiter.create(
              1.0 * (config.megabytes << 20) / partitionCount,
              (config.megabytes << 20) * config.burstFactor / partitionCount);
    }

    if (config.qps > 0) {
      this.qpsLimiter =
          RateLimiter.create(
              1.0 * config.qps / partitionCount, config.qps * config.burstFactor / partitionCount);
    }
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
