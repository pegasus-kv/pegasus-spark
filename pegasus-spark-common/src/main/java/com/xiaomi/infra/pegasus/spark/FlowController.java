package com.xiaomi.infra.pegasus.spark;

import com.revinate.guava.util.concurrent.RateLimiter;
import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FlowController {

  public static class RateLimiterConfig implements Serializable {
    private double bps;
    private double qps;
    private double burstFactor;

    public RateLimiterConfig() {
      this.bps = Long.MAX_VALUE;
      this.qps = Long.MAX_VALUE;
      this.burstFactor = 1;
    }

    public RateLimiterConfig setBps(double bps) {
      this.bps = bps;
      return this;
    }

    public RateLimiterConfig setBurstFactor(double burstFactor) {
      this.burstFactor = burstFactor;
      return this;
    }

    public RateLimiterConfig setQps(double qps) {
      this.qps = qps;
      return this;
    }

    public double getQps() {
      return qps;
    }

    public double getBps() {
      return bps;
    }

    public double getBurstFactor() {
      return burstFactor;
    }
  }

  private static final Log LOG = LogFactory.getLog(FlowController.class);

  private ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

  private RateLimiter bytesLimiter;
  private RateLimiter qpsLimiter;
  private String name;

  public FlowController(int partitionCount, double qps, double bps, double factor) {
    this.qpsLimiter = RateLimiter.create(qps / partitionCount, qps * factor / partitionCount);
    this.bytesLimiter = RateLimiter.create(bps / partitionCount, bps * factor / partitionCount);
  }

  public void acquireBytes(int bytes) {
    bytesLimiter.acquire(bytes);
  }

  public void acquireQPS() {
    qpsLimiter.acquire();
  }
}
