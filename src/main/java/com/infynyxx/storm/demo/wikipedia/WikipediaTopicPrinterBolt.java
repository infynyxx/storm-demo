package com.infynyxx.storm.demo.wikipedia;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import java.util.Map;

public class WikipediaTopicPrinterBolt extends ShellBolt implements IRichBolt {

    public WikipediaTopicPrinterBolt() {
        super("python", "wikipedia_message_parser.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("wikipedia_topic"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
