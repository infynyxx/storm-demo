package com.infynyxx.storm.demo.wikipedia;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;


/**
 * This class is responsible for defining bolts and spouts for wikipedia
 * topopoly or computing graph for wikipedia edit feeds
 */
public class WikipediaFeedTopology {

    private WikipediaFeedTopology() { }

    public static void main(String[] args) {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wikipedia_feed",
                new WikipediaSpout("#en.wikipedia"));
        builder.setBolt("wikipedia_file_appender",
                new FileAppenderBolt("/tmp/wikipedia_file_appender.json"))
                .shuffleGrouping("wikipedia_feed");

        builder.setBolt("wikipedia_message_parser",
                new WikipediaTopicPrinterBolt(),
                Runtime.getRuntime().availableProcessors())
                .shuffleGrouping("wikipedia_feed");
        builder.setBolt("wikiepedia_top_topic",
                new WikipediaTopicRankerBolt(),
                Runtime.getRuntime().availableProcessors())
                .shuffleGrouping("wikipedia_message_parser");

        final Config config = new Config();
        config.setDebug(true);

        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wikipedia_topology", config, builder.createTopology());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                cluster.shutdown();
            }
        });
    }
}
