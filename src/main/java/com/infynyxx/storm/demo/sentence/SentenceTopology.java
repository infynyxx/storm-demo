package com.infynyxx.storm.demo.sentence;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class SentenceTopology {

    private SentenceTopology() { }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setBolt("split", new SplitSentence(), 4);
        builder.setSpout("spout", new RandomSentenceSpout());

        Config config = new Config();
        config.setDebug(true);
        config.setMaxTaskParallelism(6);
        config.setNumWorkers(3);

        //LocalCluster cluster = new LocalCluster();
        StormSubmitter.submitTopology("sentence", config, builder.createTopology());
        //cluster.submitTopology("word_split", config, builder.createTopology());
    }
}
