package com.microsoft.example;

import java.io.FileWriter;
import java.util.Map;
import java.io.IOException;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class HashtagReaderBolt implements IRichBolt {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String path = "./Data.txt";
        boolean append_to_file = true;
        Status tweet = (Status) tuple.getValueByField("tweet");
        try {
            FileWriter writer = new FileWriter(path, append_to_file);
            writer.write(tweet.getText() + "\n");
            writer.close();
            for (HashtagEntity hashtage : tweet.getHashtagEntities()) {
                System.out.println("Hashtag: " + hashtage.getText());
                this.collector.emit(new Values(hashtage.getText()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}