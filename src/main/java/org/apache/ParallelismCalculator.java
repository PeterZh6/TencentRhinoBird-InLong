package org.apache;


public class ParallelismCalculator {
    private static final double RECORDS_PER_SECOND_PER_CORE = 1000.0;

    /**
     * Calculate the optimal parallelism based on the average data rate
     * 考虑细化计算的逻辑
     * @param dataPerSecond
     * @return
     */
    public static int calculateOptimalParallelism(long dataPerSecond) {
        int parallelism = (int) Math.ceil(dataPerSecond / RECORDS_PER_SECOND_PER_CORE);
        // 处理异常
        if(parallelism < 1) {
            parallelism = 1;
        }
        System.out.println("Calculated parallelism: " + parallelism);
        return parallelism;
    }
}

