package pl.edu.storm.topologies;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.aggregators.LongSum;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import pl.edu.storm.YoutubeVideosDataSpout;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

public class TotalViewsByCategoryStreamTopology {

    public static void main(String[] args) {
        try {
            runTopology();
        } catch (InvalidTopologyException | AuthorizationException | AlreadyAliveException e) {
            e.printStackTrace();
        }
    }

    public static void runTopology() throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        YoutubeVideosDataSpout spout = new YoutubeVideosDataSpout("videos_2.csv");
        StreamBuilder streamBuilder = new StreamBuilder();

        streamBuilder.newStream(spout)
                .repartition(5)
                .filter(tuple -> tuple.getIntegerByField("category_id") > 10)
                .mapToPair(tuple -> Pair.of(tuple.getIntegerByField("category_id"), tuple.getIntegerByField("views")))
                .aggregateByKey(new LongSum())
                .print();

        Config config = new Config();
        config.setDebug(false);
        StormSubmitter.submitTopology("top-viewed-in-category-stream-topology", config, streamBuilder.build());
    }

}
