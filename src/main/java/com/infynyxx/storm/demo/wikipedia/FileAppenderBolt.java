package com.infynyxx.storm.demo.wikipedia;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class FileAppenderBolt extends BaseRichBolt {

    private BufferedWriter bufferedWriter;
    private OutputCollector collector;
    private String filePath;

    public FileAppenderBolt(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        File file = new File(filePath);
        if (!file.canWrite()) {
            throw new RuntimeException(String.format("File %s is not writable", file.getAbsoluteFile()));
        }
        this.collector = collector;
        try {
            bufferedWriter = Files.newBufferedWriter(Paths.get(filePath),
                    Charset.forName("UTF-8"),
                    StandardOpenOption.APPEND);
            bufferedWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("wikipedia_feed"));
    }

    @Override
    public void execute(Tuple input) {
        String json = input.getString(0);
        try {
            bufferedWriter.write(String.valueOf(json));
            bufferedWriter.write(System.lineSeparator());
            bufferedWriter.flush();
        } catch (IOException e) {
            collector.fail(input);
            throw new RuntimeException(e);
        }
        collector.ack(input);
    }

    @Override
    public void cleanup() {
        try {
            bufferedWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
