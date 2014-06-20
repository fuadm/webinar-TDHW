/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hazelcast.webinar.tdhw;

/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import com.hazelcast.core.HazelcastInstance;
import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramData;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class PerformanceTester {
    private final HazelcastInstance instance;

    private final Stats[] allStats;
    private final int threadCount;
    private long STATS_SECONDS = 10;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ExecutorService es;

    public PerformanceTester(HazelcastInstance instance, int threadCount) {
        this.threadCount = threadCount;
        es = Executors.newFixedThreadPool(threadCount);
        this.allStats = new Stats[threadCount];
        for (int i = 0; i < threadCount; i++) {
            allStats[i] = new Stats();
        }
        this.instance = instance;
    }

    public static String[] generateStringArray(int size) {
        String[] arr = new String[size];
        for (int i = 0; i < size; i++) {
            arr[i] = (String.valueOf(i));
        }
        return arr;
    }

    public static int[] generateIntArray(int size) {
        int[] arr = new int[size];
        for (int i = 0; i < size; i++) {
            arr[i] = 0;
        }
        return arr;
    }

    public void start(Runnable operation) {
        startPrintStats();
        run(es, operation);
    }

    public void stop() {
        running.set(false);
        es.shutdown();
    }

    private void run(ExecutorService es, Runnable operation) {
        for (int i = 0; i < threadCount; i++) {
            final int tid = i;
            es.execute(new Runnable() {
                @Override
                public void run() {
                    while (running.get()) {
                        long start = System.nanoTime();
                        operation.run();
                        long end = System.nanoTime();
                        Stats stats = allStats[tid];
                        stats.op.incrementAndGet();
                        try {
                            stats.histogram.recordValue(end - start);
                        } catch (IndexOutOfBoundsException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
    }

    private void startPrintStats() {
        new Thread() {
            {
                setDaemon(true);
                setName("PrintStats." + instance.getName());
            }

            public void run() {
                final AbstractHistogram totalHistogram = new Histogram(1, TimeUnit.MINUTES.toNanos(1), 3);
                while (true) {
                    try {
                        Thread.sleep(STATS_SECONDS * 1000);
                        totalHistogram.reset();
                        long opsNow = 0;
                        for (int i = 0; i < threadCount; i++) {
                            Stats stats = allStats[i];
                            opsNow += stats.op.getAndSet(0);
                            totalHistogram.add(stats.histogram);
                            stats.histogram.reset();
                        }
                        HistogramData data = totalHistogram.getHistogramData();
                        totalHistogram.reestablishTotalCount();
                        data.outputPercentileDistribution(System.out, 1, 1000d);
                        System.out.println("Number are in micro second!");
                        System.out.println("Operations Per Second= " + opsNow / STATS_SECONDS);
                        System.out.println();
                        System.out.println();
                    } catch (InterruptedException ignored) {
                        return;
                    }
                }
            }
        }.start();
    }

    private static class Stats {
        final AbstractHistogram histogram = new AtomicHistogram(1, TimeUnit.MINUTES.toNanos(1), 3);
        final AtomicLong op = new AtomicLong();
    }
}

