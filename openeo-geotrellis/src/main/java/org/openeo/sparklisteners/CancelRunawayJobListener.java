package org.openeo.sparklisteners;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.spark.JobExecutionStatus;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

public class CancelRunawayJobListener extends SparkListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(CancelRunawayJobListener.class);

    private final Supplier<SparkContext> sc;
    private final Duration timeout;

    public CancelRunawayJobListener(Duration timeout, Supplier<SparkContext> sc) {
        this.timeout = timeout;
        this.sc = sc;

        LOGGER.info("initialized with timeout {}", timeout);
    }

    @SuppressWarnings("unused") // invoked through --conf spark.extraListeners=
    public CancelRunawayJobListener() {
        this(Optional.ofNullable(System.getProperty("openeo.runawaysparkjobtimeout"))
                        .map(Long::parseLong)
                        .map(Duration::ofSeconds)
                        .orElse(Duration.ofMinutes(15)),
                Suppliers.memoize(SparkContext::getOrCreate)); // app won't start unless done lazily
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        int jobId = jobStart.jobId();

        long start = jobStart.time();
        long end = start + timeout.toMillis();

        Thread canceller = new Thread(() -> {
            LOGGER.debug("scheduling cancellation of runaway job {}...", jobId);

            try {
                while (!Thread.currentThread().isInterrupted()) {
                    TimeUnit.MILLISECONDS.sleep(timeout.toMillis() / 10);

                    if (hasCompleted(jobId)) {
                        LOGGER.debug("interrupted cancelling of completed (non-runaway) job {}", jobId);
                        return;
                    }

                    boolean timeoutExpired = System.currentTimeMillis() >= end;

                    if (timeoutExpired) {
                        cancelJob(jobId);
                        return;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.debug("interrupted cancelling of runaway job {}", jobId);
            }
        });

        canceller.setDaemon(true);
        canceller.start();
    }

    private boolean hasCompleted(int jobId) {
        Option<SparkJobInfo> jobInfo = sc.get().statusTracker().getJobInfo(jobId);

        if (jobInfo.isEmpty()) return true;

        JobExecutionStatus status = jobInfo.get().status();
        return status.equals(JobExecutionStatus.SUCCEEDED) || status.equals(JobExecutionStatus.FAILED);
    }

    private void cancelJob(int jobId) {
        LOGGER.debug("cancelling runaway job {}...", jobId);
        String msg = String.format("runaway job %d cancelled after %s", jobId, timeout);
        sc.get().cancelJob(jobId, msg);
        LOGGER.info(msg);
    }
}
