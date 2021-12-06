package org.openeo.sparklisteners;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark listener which will kill the driver process in case the service is ran in client mode as then Yarn cannot
 * kill the driver when the Spark application is killed via the Yarn.
 */
public class KillDriverListener extends SparkListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(KillDriverListener.class);

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        LOGGER.info("Killing driver as the application was killed");
        System.exit(0);
    }

}
