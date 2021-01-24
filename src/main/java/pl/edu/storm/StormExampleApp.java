package pl.edu.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

public class StormExampleApp {

    public static void main(String[] args) {
        try {
            runTopology();
        } catch (InvalidTopologyException | AuthorizationException | AlreadyAliveException e) {
            e.printStackTrace();
        }
    }

    public static void runTopology() throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        boolean useYoutubeData = true;

        TopologyBuilder builder = new TopologyBuilder();

        if(useYoutubeData) {
            IRichSpout spout = new YoutubeDataSpout("US-youtube-sample.json");
            builder.setSpout("youtubeDataSpout", spout);

            IBasicBolt file = new PrintingBolt();
            builder.setBolt("printingBolt", file)
                    .shuffleGrouping("youtubeDataSpout");
        } else {
            IRichSpout spout = new RandomNumberSpout();
            builder.setSpout("randomNumberSpout", spout);

            IBasicBolt filtering = new FilteringBolt();
            builder.setBolt("filteringBolt", filtering)
                    .shuffleGrouping("randomNumberSpout");

            BaseWindowedBolt aggregating = new AggregatingBolt()
                    .withTimestampField("timestamp")
                    .withLag(BaseWindowedBolt.Duration.seconds(1))
                    .withWindow(BaseWindowedBolt.Duration.seconds(5));
            builder.setBolt("aggregatingBolt", aggregating)
                    .shuffleGrouping("filteringBolt");

            IBasicBolt file = new PrintingBolt();
            builder.setBolt("printingBolt", file)
                    .shuffleGrouping("aggregatingBolt");
        }

        Config config = new Config();
        config.setDebug(false);
        StormSubmitter.submitTopology("adzd-topology", config, builder.createTopology());
    }
}
