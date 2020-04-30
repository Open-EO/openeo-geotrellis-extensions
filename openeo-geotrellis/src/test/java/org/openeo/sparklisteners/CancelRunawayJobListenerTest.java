package org.openeo.sparklisteners;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;

public class CancelRunawayJobListenerTest {

    private static final long timeoutInMilliseconds = 5000;
    private static JavaSparkContext jsc;

    @BeforeClass
    public static void setupSpark() {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName(CancelRunawayJobListener.class.getName());

        SparkContext sc = SparkContext.getOrCreate(conf);
        jsc = JavaSparkContext.fromSparkContext(sc);

        SparkListener cancelRunawayJobListener =
                new CancelRunawayJobListener(Duration.ofMillis(timeoutInMilliseconds), () -> sc);

        sc.addSparkListener(cancelRunawayJobListener);
    }

    @AfterClass
    public static void tearDownSpark() {
        jsc.stop();
    }

    @Test
    public void regularJobSucceeds() {
        JavaRDD<Integer> ints = jsc.parallelize(asList(1, 2, 3));

        int sum = ints.reduce(Integer::sum);

        assertEquals(6, sum);
    }

    @Test(expected = SparkException.class, timeout = timeoutInMilliseconds * 2)
    public void runawayJobIsCancelled() {
        JavaRDD<Integer> ints = jsc.parallelize(asList(1, 2, 3));

        int slowSum = ints
                .mapPartitions(is -> {
                    TimeUnit.MILLISECONDS.sleep(timeoutInMilliseconds * 2);
                    return is;
                })
                .reduce(Integer::sum);

        assertEquals(6, slowSum);
    }
}
