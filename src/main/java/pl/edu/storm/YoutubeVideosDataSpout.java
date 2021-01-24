package pl.edu.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class YoutubeVideosDataSpout extends BaseRichSpout {
    private final String dataFilePath;

    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private Map<String, Integer> headers = new HashMap<>();
    private SpoutOutputCollector collector;
    private BufferedReader reader;
    private boolean finished = false;

    public YoutubeVideosDataSpout(String dataFilePath) {
        this.dataFilePath = dataFilePath;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
        try {
            InputStream in = getClass().getClassLoader().getResourceAsStream(dataFilePath);
            reader = new BufferedReader(new InputStreamReader(in));
            List<String> arr = Arrays.asList(reader.readLine().split(","));
            IntStream.range(0, arr.size()).forEach(i -> headers.put(arr.get(i), i));
        } catch (IOException e) {
            e.printStackTrace();
        }
        collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        try {
            if(!finished) {
                String input;
                if((input = reader.readLine()) != null) {
                    String[] line = input.split(",");
                    collector.emit(new Values(
                            Integer.parseInt(line[headers.get("category_id")]),
                            Integer.parseInt(line[headers.get("views")]),
                            Integer.parseInt(line[headers.get("likes")]),
                            simpleDateFormat.parse(line[headers.get("publish_time")]).getTime()));
                } else {
                    reader.close();
                    finished = true;
                }
            }
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("category_id", "views", "likes", "publish_time"));
    }

}