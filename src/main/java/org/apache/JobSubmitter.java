package org.apache;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.StandaloneClusterId;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.DataRateMonitorFunction;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.ParallelismCalculator.calculateOptimalParallelism;

/**
 * 对应 org.apache.inlong.manager.plugin.flink.FlinkService 中的 submitJobBySavepoint 方法
 */
public class JobSubmitter {


    /**
     * Submits a Flink job by a given savepoint with dynamically calculated parallelism based on historical data rates.
     */
    private String submitJobBySavepoint(FlinkInfo flinkInfo, SavepointRestoreSettings settings) throws Exception {
        // Calculate the average data rate from historical data and determine optimal parallelism
        double averageDataRate = DataRateMonitorFunction.getHistoricalDataRateAverage();
        int desiredParallelism = calculateOptimalParallelism((int) Math.ceil(averageDataRate / 1000));

//unchanged lines start 下面的到"unchanged lines end"之前都没动过
        List<URL> connectorJars = flinkInfo.getConnectorJarPaths().stream().map(p -> {
            try {
                return new File(p).toURI().toURL();
            } catch (MalformedURLException e) {
                System.err.println("Malformed URL: " + e.getMessage());
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());

        Configuration configuration = getFlinkConfiguration(flinkInfo.getEndpoint());

        File jarFile = new File(flinkInfo.getLocalJarPath());
        PackagedProgram program = PackagedProgram.newBuilder()
                .setConfiguration(configuration)
                .setEntryPointClassName(Constants.ENTRYPOINT_CLASS)
                .setJarFile(jarFile)
                .setUserClassPaths(connectorJars)
                .setArguments(genProgramArgs(flinkInfo, flinkConfig))
                .setSavepointRestoreSettings(settings)
                .build();
//unchanged lines end

        // 在这里设置优化过的并行度，用该并行度提交job
        JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, configuration, desiredParallelism, false);
        jobGraph.addJars(connectorJars);

        RestClusterClient<StandaloneClusterId> client = getFlinkClientService(configuration).getFlinkClient();
        CompletableFuture<JobID> result = client.submitJob(jobGraph);
        return result.get().toString();
    }

}