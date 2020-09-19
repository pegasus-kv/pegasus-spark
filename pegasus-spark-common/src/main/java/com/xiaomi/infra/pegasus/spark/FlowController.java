package com.xiaomi.infra.pegasus.spark;

import com.revinate.guava.util.concurrent.RateLimiter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FlowController {

  private static final Log LOG = LogFactory.getLog(FlowController.class);
  private int partitionCount;

  private RateLimiter bytesLimiter;
  private RateLimiter qpsLimiter;

  public FlowController() {
    this.bytesLimiter = RateLimiter.create(Long.MAX_VALUE);
    this.qpsLimiter = RateLimiter.create(Long.MAX_VALUE);
  }

  public void withBytesLimiter(double permitsPerSecond, double maxBurstSeconds) {
    bytesLimiter =
        RateLimiter.create(permitsPerSecond / partitionCount, maxBurstSeconds / partitionCount);
  }

  public void withQPSLimiter(double permitsPerSecond, double maxBurstSeconds) {
    qpsLimiter =
        RateLimiter.create(permitsPerSecond / partitionCount, maxBurstSeconds / partitionCount);
  }

  public void withLimiterRateMinitor(int periodSeconds, String message) {
    ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutor.schedule(
        () ->
            LOG.info(
                message
                    + " current rate = "
                    + getQPSRate()
                    + "/sec and "
                    + getBytesRate()
                    + "byte/sec"),
        periodSeconds,
        TimeUnit.SECONDS);
  }

  public void withPartitionCount(int partitionCount) {
    this.partitionCount = partitionCount;
  }

  public void acquireBytes(int bytes) {
    bytesLimiter.acquire(bytes);
  }

  public void acquireQPS() {
    qpsLimiter.acquire();
  }

  public double getQPSRate() {
    return qpsLimiter.getRate();
  }

  public double getBytesRate() {
    return bytesLimiter.getRate();
  }
}
