package pl.edu.storm.topologies;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
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

public class TopViewedInCategoryTopology {

    public static void main(String[] args) {
        try {
            runTopology();
        } catch (InvalidTopologyException | AuthorizationException | AlreadyAliveException e) {
            e.printStackTrace();
        }
    }

    public static void runTopology() throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        TopologyBuilder builder = new TopologyBuilder();

        YoutubeVideosDataSpout spout = new YoutubeVideosDataSpout("videos_2.csv");
        builder.setSpout("TopViewedInCategoryTopology.youtubeVideosSpout", spout);

        builder.setBolt("TopViewedInCategoryTopology.filteringBolt", new FilteringBolt(10))
                .shuffleGrouping("TopViewedInCategoryTopology.youtubeVideosSpout");

        BaseWindowedBolt aggregating = new AggregatingBolt()
                .withTimestampField("publish_time")
                .withWindow(BaseWindowedBolt.Duration.seconds(10));

        builder.setBolt("TopViewedInCategoryTopology.aggregatingBolt", aggregating)
                .shuffleGrouping("TopViewedInCategoryTopology.filteringBolt");

        builder.setBolt("TopViewedInCategoryTopology.resultPrintingBolt", new PrintingBolt())
                .shuffleGrouping("TopViewedInCategoryTopology.aggregatingBolt");

        Config config = new Config();
        config.setDebug(false);
        StormSubmitter.submitTopology("top-viewed-in-category-topology-2", config, builder.createTopology());

    }

    public static class FilteringBolt extends BaseBasicBolt {

        private final int categoryId;

        public FilteringBolt(int categoryId) {
            this.categoryId = categoryId;
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            int operation = tuple.getIntegerByField("category_id");
            if (operation == categoryId) {
                basicOutputCollector.emit(tuple.getValues());
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("category_id", "views", "likes", "publish_time"));
        }
    }

    private static class AggregatingBolt extends BaseWindowedBolt {

        private OutputCollector outputCollector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.outputCollector = collector;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("category_id", "maxViews", "beginningTimestamp", "endTimestamp"));
        }

        @Override
        public void execute(TupleWindow tupleWindow) {
            List<Tuple> tuples = tupleWindow.get();
            tuples.sort(Comparator.comparing(this::getTimestamp));

            OptionalInt maxOpt = tuples.stream()
                    .mapToInt(tuple -> tuple.getIntegerByField("views"))
                    .max();
            Long beginningTimestamp = getTimestamp(tuples.get(0));
            Long endTimestamp = getTimestamp(tuples.get(tuples.size() - 1));

            Values values = new Values(tuples.get(0).getIntegerByField("category_id"), maxOpt.orElse(0), beginningTimestamp, endTimestamp);
            outputCollector.emit(values);
        }

        private Long getTimestamp(Tuple tuple) {
            return tuple.getLongByField("publish_time");
        }
    }

    private static class PrintingBolt extends BaseBasicBolt {

        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            System.out.println(tuple.getValues());
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }
}
