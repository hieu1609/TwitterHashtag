package com.microsoft.example;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.apache.storm.tuple.Fields;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class HashtagCounterBolt implements IRichBolt {
    Map<String, Integer> counterMap;
    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context,
                        OutputCollector collector) {
        this.counterMap = new HashMap<String, Integer>();
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String key = tuple.getString(0);
        if (!counterMap.containsKey(key)) {
            counterMap.put(key, 1);
        } else {
            Integer c = counterMap.get(key) + 1;
            counterMap.put(key, c);
        }
        collector.ack(tuple);
    }


    @Override
    public void cleanup() {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
        System.out.println( sdf.format(cal.getTime()) );
        System.out.println( TwitterHashtagStorm.time );
        int seconds = TwitterHashtagStorm.time / 1000;
        String path = "./Hashtag/Hashtag " + sdf.format(cal.getTime()) + " " + seconds + " seconds.txt";
        boolean append_to_file = false;
        try {
            FileWriter writer = new FileWriter(path, append_to_file);
            int i = 1;
            for (Map.Entry<String, Integer> entry : counterMap.entrySet()) {
                System.out.println("Hashtag " + i + ": #" + entry.getKey() + " : " + entry.getValue());
                writer.write("Hashtag " + i + ": #" + entry.getKey() + " : " + entry.getValue() + "\n");
                i++;
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
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