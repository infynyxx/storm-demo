package com.infynyxx.storm.demo.wikipedia;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class WikipediaSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;

    private SpoutOutputCollector collector;
    private WikipediaFeed wikipediaFeed;
    private String channelName;
    private LinkedBlockingDeque<WikipediaFeed.WikipediaFeedEvent> feedEventLinkedBlockingDeque;
    private ExecutorService executorService;

    public WikipediaSpout(String channelName) {
        this.channelName = channelName;
        this.feedEventLinkedBlockingDeque = new LinkedBlockingDeque<>();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("wikipedia_feed"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        wikipediaFeed = new WikipediaFeed("irc.wikimedia.org", 6667);
        wikipediaFeed.start();
        executorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                .setNameFormat("FeedListenerWorker-%d")
                .build());
        executorService.submit(new FeedListenerWorker());
    }

    @Override
    public void nextTuple() {
        final WikipediaFeed.WikipediaFeedEvent event = feedEventLinkedBlockingDeque.poll();
        if (event != null) {
            collector.emit(new Values(WikipediaFeed.WikipediaFeedEvent.toJson(event)));
        }
    }

    @Override
    public void close() {
        wikipediaFeed.stop();
        executorService.shutdown();
    }

    private class FeedListenerWorker implements Runnable {

        @Override
        public void run() {
            wikipediaFeed.listen(channelName, new WikipediaFeed.WikipediaFeedListener() {
                @Override
                public void onEvent(WikipediaFeed.WikipediaFeedEvent event) {
                    feedEventLinkedBlockingDeque.add(event);
                }
            });
        }
    }
}
