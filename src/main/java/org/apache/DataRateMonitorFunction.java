package org.apache;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.util.Collector;

public class DataRateMonitorFunction extends RichFlatMapFunction<String, String> {
    private transient int recordCounter = 0;
    private transient long lastTimeMillis = System.currentTimeMillis();

    private double avgDataRate = 0;

    /**
     * 尝试使用 Flink 的 Metric 系统来监控数据速率
     * 当 Flink 任务启动并且各个算子开始准备处理流数据时，open 方法将为每个任务的算子实例被调用
     * 计算每秒处理的记录数，并将其作为一个 Gauge 指标暴露给 Flink 的 Metric 系统
     * 并且将数据速率保存到数据库或文件中，先尝试保存到一个变量中
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        getRuntimeContext()
                .getMetricGroup()
                .gauge("recordsPerSecond", new Gauge<Double>() {
                    @Override
                    public Double getValue() {
                        long currentTimeMillis = System.currentTimeMillis();
                        double elapsedSeconds = (currentTimeMillis - lastTimeMillis) / 1000.0;
                        if (elapsedSeconds > 0) {
                            double recordsPerSecond = recordCounter / elapsedSeconds;
                            recordCounter = 0;
                            lastTimeMillis = currentTimeMillis;
                            saveDataRate(recordsPerSecond);  // Save the data rate for historical analysis
                            return recordsPerSecond;
                        }
                        return 0.0;
                    }
                });
    }

    private void saveDataRate(double rate) {
        // Code to save the rate to a database or a file
        //now trying to save it in a variable
        avgDataRate = (avgDataRate + rate) / 2;
    }

    public static double getHistoricalDataRateAverage() {
        // Code to get the historical data rate average
        return 0.0;
    }

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        recordCounter++;
        out.collect(value);
    }
}
