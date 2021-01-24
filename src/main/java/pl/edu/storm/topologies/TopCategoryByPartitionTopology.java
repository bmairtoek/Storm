package pl.edu.storm.topologies;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import pl.edu.storm.SumAggregator;
import pl.edu.storm.YoutubeVideosDataSpout;

import java.util.Arrays;

public class TopCategoryByPartitionTopology {

    public static void main(String[] args) {
        try {
            runTopology();
        } catch (InvalidTopologyException | AuthorizationException | AlreadyAliveException e) {
            e.printStackTrace();
        }
    }

    public static void runTopology() throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        YoutubeVideosDataSpout spout = new YoutubeVideosDataSpout("videos.csv");

        TridentTopology topology = new TridentTopology();

        topology.newStream("youtubeDataSpout", spout)
                .groupBy(new Fields("category_id"))
                .partitionAggregate(new Fields("views"), new SumAggregator(), new Fields("sum"))
                .toStream()
                .parallelismHint(5)
                .peek((Consumer) input -> System.out.println("AGGREGATE RESULTS: " + Arrays.toString(input.toArray())));

        Config config = new Config();
        config.setNumEventLoggers(3);
        config.setDebug(false);
        StormSubmitter.submitTopology("trident-topology", config, topology.build());

    }
}
