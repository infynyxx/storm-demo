package com.infynyxx.storm.demo.wikipedia;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import org.schwering.irc.lib.IRCConnection;
import org.schwering.irc.lib.IRCEventListener;
import org.schwering.irc.lib.IRCModeParser;
import org.schwering.irc.lib.IRCUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class WikipediaFeed {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(WikipediaFeed.class);
    private static final Random RANDOM = new Random();
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private final Map<String, Set<WikipediaFeedListener>> channelListeners;
    private final String host;
    private final int port;
    private final IRCConnection conn;
    private final String nick;

    public WikipediaFeed(String host, int port) {
        this.channelListeners = new HashMap<>();
        this.host = host;
        this.port = port;
        this.nick = String.format("infynyxx-bot-%d", Math.abs(RANDOM.nextInt()));
        this.conn = new IRCConnection(host, new int[] { port }, "", nick, nick, nick);
        this.conn.addIRCEventListener(new WikipediaFeedIrcListener());
        this.conn.setEncoding("UTF-8");
        this.conn.setPong(true);
        this.conn.setColors(false);
    }

    public void start() {
        try {
            this.conn.connect();
        } catch (IOException e) {
            throw new RuntimeException("Unable to connect to " + host + ":" + port + ".", e);
        }
    }

    public void stop() {
        this.conn.interrupt();

        try {
            this.conn.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(String.format("Interrupted while trying to shutdown IRC connection for %s:%d",
                    host,
                    port), e);
        }

        if (this.conn.isAlive()) {
            throw new RuntimeException(String.format("Unable to shutdown IRC connection for %s:%d", host, port));
        }
    }

    public void listen(String channel, WikipediaFeedListener listener) {
        Set<WikipediaFeedListener> listeners = channelListeners.get(channel);

        if (listeners == null) {
            listeners = new HashSet<>();
            channelListeners.put(channel, listeners);
            join(channel);
        }

        listeners.add(listener);
    }

    public void unlisten(String channel, WikipediaFeedListener listener) {
        Set<WikipediaFeedListener> listeners = channelListeners.get(channel);

        if (listeners == null) {
            throw new RuntimeException("Trying to unlisten to a channel that has no listeners in it.");
        } else if (!listeners.contains(listener)) {
            throw new RuntimeException("Trying to unlisten to a channel that listener is not listening to.");
        }

        listeners.remove(listener);

        if (listeners.size() == 0) {
            leave(channel);
        }
    }

    public void join(String channel) {
        conn.send("JOIN " + channel);
    }

    public void leave(String channel) {
        conn.send("PART " + channel);
    }

    public class WikipediaFeedIrcListener implements IRCEventListener {
        public void onRegistered() {
            LOGGER.info("Connected");
        }

        public void onDisconnected() {
            LOGGER.info("Disconnected");
        }

        public void onError(String msg) {
            LOGGER.info("Error: " + msg);
        }

        public void onError(int num, String msg) {
            LOGGER.info("Error #" + num + ": " + msg);
        }

        public void onInvite(String chan, IRCUser u, String nickPass) {
            LOGGER.info(chan + "> " + u.getNick() + " invites " + nickPass);
        }

        public void onJoin(String chan, IRCUser u) {
            LOGGER.info(chan + "> " + u.getNick() + " joins");
        }

        public void onKick(String chan, IRCUser u, String nickPass, String msg) {
            LOGGER.info(chan + "> " + u.getNick() + " kicks " + nickPass);
        }

        public void onMode(IRCUser u, String nickPass, String mode) {
            LOGGER.info("Mode: " + u.getNick() + " sets modes " + mode + " " + nickPass);
        }

        public void onMode(String chan, IRCUser u, IRCModeParser mp) {
            LOGGER.info(chan + "> " + u.getNick() + " sets mode: " + mp.getLine());
        }

        public void onNick(IRCUser u, String nickNew) {
            LOGGER.info("Nick: " + u.getNick() + " is now known as " + nickNew);
        }

        public void onNotice(String target, IRCUser u, String msg) {
            LOGGER.info(target + "> " + u.getNick() + " (notice): " + msg);
        }

        public void onPart(String chan, IRCUser u, String msg) {
            LOGGER.info(chan + "> " + u.getNick() + " parts");
        }

        public void onPrivmsg(String chan, IRCUser u, String msg) {
            Set<WikipediaFeedListener> listeners = channelListeners.get(chan);

            if (listeners != null) {
                WikipediaFeedEvent event = new WikipediaFeedEvent(System.currentTimeMillis(), chan, u.getNick(), msg);

                for (WikipediaFeedListener listener : listeners) {
                    listener.onEvent(event);
                }
            }

            LOGGER.debug(chan + "> " + u.getNick() + ": " + msg);
        }

        public void onQuit(IRCUser u, String msg) {
            LOGGER.info("Quit: " + u.getNick());
        }

        public void onReply(int num, String value, String msg) {
            LOGGER.info("Reply #" + num + ": " + value + " " + msg);
        }

        public void onTopic(String chan, IRCUser u, String topic) {
            LOGGER.info(chan + "> " + u.getNick() + " changes topic into: " + topic);
        }

        public void onPing(String p) {
        }

        public void unknown(String a, String b, String c, String d) {
            LOGGER.warn("UNKNOWN: " + a + " " + b + " " + c + " " + d);
        }
    }

    public interface WikipediaFeedListener {
        void onEvent(WikipediaFeedEvent event);
    }

    public static final class WikipediaFeedEvent {
        private final long time;
        private final String channel;
        private final String source;
        private final String rawEvent;

        public WikipediaFeedEvent(long time, String channel, String source, String rawEvent) {
            this.time = time;
            this.channel = channel;
            this.source = source;
            this.rawEvent = rawEvent;
        }

        public WikipediaFeedEvent(Map<String, Object> jsonObject) {
            this((Long) jsonObject.get("time"),
                    (String) jsonObject.get("channel"),
                    (String) jsonObject.get("source"),
                    (String) jsonObject.get("raw"));
        }

        public long getTime() {
            return time;
        }

        public String getChannel() {
            return channel;
        }

        public String getSource() {
            return source;
        }

        public String getRawEvent() {
            return rawEvent;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(time, channel, source, rawEvent);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            WikipediaFeedEvent other = (WikipediaFeedEvent) obj;
            if (channel == null) {
                if (other.channel != null) {
                    return false;
                }
            } else if (!channel.equals(other.channel)) {
                return false;
            }

            if (rawEvent == null) {
                if (other.rawEvent != null) {
                    return false;
                }
            } else if (!rawEvent.equals(other.rawEvent)) {
                return false;
            }

            if (source == null) {
                if (other.source != null) {
                    return false;
                }
            } else if (!source.equals(other.source)) {
                return false;
            }

            if (time != other.time) {
                return false;
            }

            return true;
        }

        @Override
        public String toString() {
            return String.format("WikipediaFeedEvent [time=%d, channel=%s, source=%s, rawEvent=%s]",
                    time,
                    channel,
                    source,
                    rawEvent);
        }

        public String toJson() {
            return toJson(this);
        }

        public static Map<String, Object> toMap(WikipediaFeedEvent event) {
            Map<String, Object> jsonObject = new HashMap<>();

            jsonObject.put("time", event.getTime());
            jsonObject.put("channel", event.getChannel());
            jsonObject.put("source", event.getSource());
            jsonObject.put("raw", event.getRawEvent());

            return jsonObject;
        }

        public static String toJson(WikipediaFeedEvent event) {
            Map<String, Object> jsonObject = toMap(event);

            try {
                return jsonMapper.writeValueAsString(jsonObject);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unchecked")
        public static WikipediaFeedEvent fromJson(String json) {
            try {
                return new WikipediaFeedEvent((Map<String, Object>) jsonMapper.readValue(json, Map.class));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
