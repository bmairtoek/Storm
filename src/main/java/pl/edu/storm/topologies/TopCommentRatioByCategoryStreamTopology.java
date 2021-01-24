package pl.edu.storm.topologies;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.CombinerAggregator;
import org.apache.storm.tuple.Tuple;
import pl.edu.storm.YoutubeVideosDataSpout;

public class TopCommentRatioByCategoryStreamTopology {

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
                .mapToPair(TopCommentRatioByCategoryStreamTopology::mapToPair)
                .aggregateByKey(new PairMaxCombiner())
                .print();

        Config config = new Config();
        config.setDebug(false);
        StormSubmitter.submitTopology("top-comment-ratio-by-category-stream-topology", config, streamBuilder.build());
    }

    private static Pair<Integer, Pair<String, Double>> mapToPair(Tuple tuple) {
        double ratio = Double.valueOf(tuple.getIntegerByField("comment_count")) / tuple.getIntegerByField("views");
        return Pair.of(tuple.getIntegerByField("category_id"), Pair.of(tuple.getStringByField("title"), ratio));
    }

    private static class PairMaxCombiner implements CombinerAggregator<Pair<String, Double>, Pair<String, Double>, Pair<String, Double>> {

        @Override
        public Pair<String, Double> init() {
            return Pair.of("", 0.0);
        }

        @Override
        public Pair<String, Double> apply(Pair<String, Double> stringDoublePair, Pair<String, Double> stringDoublePair2) {
            return stringDoublePair.value2 > stringDoublePair2.value2 ? stringDoublePair : stringDoublePair2;
        }

        @Override
        public Pair<String, Double> merge(Pair<String, Double> stringDoublePair, Pair<String, Double> a1) {
            return stringDoublePair.value2 > a1.value2 ? stringDoublePair : a1;
        }

        @Override
        public Pair<String, Double> result(Pair<String, Double> stringDoublePair) {
            return stringDoublePair;
        }
    }

}
