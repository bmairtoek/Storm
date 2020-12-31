package pl.edu.storm;

import org.apache.storm.shade.org.json.simple.JSONArray;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.*;
import java.util.Map;
import java.util.stream.Collectors;

public class YoutubeDataSpout extends BaseRichSpout {
    private final String dataFilePath = "US-youtube-sample.json";

    private JSONArray data;
    private int dataPointer = 0;
    private SpoutOutputCollector collector;

    @Override
    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
        try {
            InputStream in = getClass().getClassLoader().getResourceAsStream(dataFilePath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String content = reader.lines().collect(Collectors.joining());
            JSONParser parser = new JSONParser();
            data = (JSONArray) ((JSONObject) parser.parse(content)).get("items");
        } catch (Exception e) {
            System.out.println(e);
            throw new RuntimeException(e);
        }
        collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(1000);
        String title = (String) ((JSONObject) ((JSONObject) data.get(dataPointer)).get("snippet")).get("title");
        long timestamp = System.currentTimeMillis();

        Values values = new Values(title, timestamp);
        collector.emit(values);
        dataPointer = (dataPointer + 1) % data.size();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("title", "timestamp"));
    }
}