package pl.edu.storm.topologies;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import pl.edu.storm.YoutubeVideosDataSpout;
import pl.edu.storm.operations.MaxCombiner;

import java.util.Arrays;

public class LikesByCategoryPersistentTopology {

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
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("likes"), new MaxCombiner(), new Fields("max"))
                .parallelismHint(5)
                .newValuesStream()
                .peek((Consumer) input -> System.out.println("PERSISTENT AGGREGATE RESULTS: " + Arrays.toString(input.toArray())));

        Config config = new Config();
        config.setNumEventLoggers(3);
        config.setDebug(false);
        StormSubmitter.submitTopology("likes-by-category-topology", config, topology.build());

    }
}
