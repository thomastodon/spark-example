import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class StatusTracker {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
            .builder()
            .appName("StatusTracker")
            .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());

        JavaRDD<Integer> rdd = sparkContext
            .parallelize(Arrays.asList(1, 2, 3, 4, 5))
            .map(new IdentityWithDelay<>());

        JavaFutureAction<List<Integer>> jobFuture = rdd.collectAsync();

        while (!jobFuture.isDone()) {
            Thread.sleep(1000);
            List<Integer> jobIds = jobFuture.jobIds();

            if (jobIds.isEmpty()) continue;

            Integer currentJobId = jobIds.get(jobIds.size() - 1);
            SparkJobInfo jobInfo = sparkContext.statusTracker().getJobInfo(currentJobId);
            SparkStageInfo stageInfo = sparkContext.statusTracker().getStageInfo(jobInfo.stageIds()[0]);

            System.out.println("tasks:"
                + " total=" + stageInfo.numTasks()
                + " active=" + stageInfo.numActiveTasks()
                + " completed=" + stageInfo.numCompletedTasks()
            );
        }

        System.out.println("jobFuture: " + jobFuture.get());
        spark.stop();
    }

    public static final class IdentityWithDelay<T> implements Function<T, T> {

        @Override
        public T call(T x) throws Exception {
            Thread.sleep(2 * 1000);
            return x;
        }
    }
}