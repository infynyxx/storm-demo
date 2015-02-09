package com.infynyxx.storm.demo.wikipedia;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class WikipediaTopicRankerBolt extends BaseRichBolt {

    private static final long serialVersionUID = -8447525895532302198L;
    private static final Logger LOG = LoggerFactory.getLogger(WikipediaTopicRankerBolt.class);

    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
    private static final int DEFAULT_COUNT = 10;

    private final int itemsCount;
    private final int emitsFrequencyInSeconds;
    private final Rankings rankings;
    private final Date date = new Date();

    private OutputCollector collector;

    private ScheduledExecutorService scheduler;

    public WikipediaTopicRankerBolt() {
        this(DEFAULT_COUNT, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public WikipediaTopicRankerBolt(int topN) {
        this(topN, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public WikipediaTopicRankerBolt(int topN, int emitFrequencyInSeconds) {
        checkArgument(topN >= 1, "topN must be >= 1");
        checkArgument(emitFrequencyInSeconds >= 1, "emitFrequencyInSeconds must be >= 1");
        itemsCount = topN;
        emitsFrequencyInSeconds = emitFrequencyInSeconds;
        rankings = new Rankings(topN);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("wikipedia_message_parser"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setNameFormat("ranking-printer-%d")
                .setDaemon(true)
                .build());
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (rankings.size() > 0) {
                    LOG.info("RANKINGS: " + rankings.getRankings());
                }
            }
        }, 1, 5, TimeUnit.SECONDS); // print every 5 seconds
    }

    @Override
    public void execute(Tuple tuple) {
        String title = tuple.getString(0);
        Rankable rankable = new RankableObject(title, title.length());
        rankings.updateWith(rankable);
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        scheduler.shutdown();
    }
}
