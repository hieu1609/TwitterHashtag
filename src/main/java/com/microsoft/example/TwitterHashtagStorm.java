package com.microsoft.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TwitterHashtagStorm {
    public static int time = 100000;

    public static void main(String[] args) throws Exception {
        String consumerKey = "pazf7nlIgMXstqodNOPCFYfD3";
        String consumerSecret = "lfVOOvZ6e9jHSQ3vCWwkx6cTL701Lvum2y6LgdAwYdFUdtsROo";
        String accessToken = "1194817985233350663-i4JFqPlOaJYSrsq5K9xnPbcyQ1SwHr";
        String accessTokenSecret = "1RTbXWhYsnZQg8BPsBiMZTzdReAZb1K8nFphbuVxHW78i";
        String[] keyWords = {"tweet","hello","hadoop", "ico", "month", "crypto", "day", "medicaid", "top", "miss", "stop", "apink",
                "history", "photography", "iwd2019", "crypto", "happy", "women", "water", "fan", "fans", "love", "scores", "mtp",
                "birthday", "international", "olympics", "pets", "friends", "home", "world", "funny", "contest", "game", "exo","bts",
                "motivation", "giveaway", "tuesday", "travel", "bbc", "people", "photo", "win", "goals", "fitness", "start", "bigbang",
                "video", "trending", "go", "more", "shop", "like", "facebook", "youtube", "amazon", "gmail", "google",  "blackpink",
                "ebay", "yahoo", "weather", "craigslist", "mail", "maps", "walmart", "netflix", "translate", "pokemon", "play", "kpop",
                "news", "depot", "cnn", "hotmail", "fox", "calculator", "login", "drive", "tracking", "gps", "lowes", "paypal", "docs",
                "instagram", "target", "bank", "pinterest", "twitter", "entertainment", "indeed", "buy", "best", "speed", "test",
                "trump", "sports", "nba", "mu", "bing", "pizza", "dominos", "you", "restaurants", "outlook", "timer", "time", "korea",
                "2019", "2020", "cafe", "cafe", "mobile", "coffee", "match", "mocha", "latte", "espresso", "cappuccino", "us", "uk"};
        Config config = new Config();
        config.setDebug(true);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-spout", new TwitterSampleSpout(consumerKey,
                consumerSecret, accessToken, accessTokenSecret, keyWords));
        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt())
                .shuffleGrouping("twitter-spout");
        builder.setBolt("twitter-hashtag-counter-bolt",
                new HashtagCounterBolt()).fieldsGrouping(
                "twitter-hashtag-reader-bolt", new Fields("hashtag"));
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TwitterHashtagStorm", config,
                builder.createTopology());
        Thread.sleep(time);
        cluster.shutdown();
    }
}