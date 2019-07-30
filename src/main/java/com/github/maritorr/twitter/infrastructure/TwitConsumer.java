package com.github.maritorr.twitter.infrastructure;

import com.github.maritorr.twitter.kafka.infrastructure.ProducerWithCallback;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(TwitConsumer.class);

    private ProducerWithCallback producer;
    private BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
    private BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
    private Properties properties;

    @PostConstruct
    public void setUp() throws IOException {
        producer = new ProducerWithCallback();

        properties = new Properties();
        properties.load(TwitConsumer.class.getResourceAsStream("/twitter-credentials.properties"));
    }

    public void run() {
        try {
            setUp();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("twitter", "api");
        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(
                properties.getProperty("twitter.consumer.key"),
                properties.getProperty("twitter.consumer.secret"),
                properties.getProperty("twitter.token"),
                properties.getProperty("twitter.secret"));

        // Creating the client
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-maritorr")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);

        Client hosebirdClient = builder.build();
        hosebirdClient.connect();

        CountDownLatch latch = new CountDownLatch(1);

        new Thread(() -> {
            while(!hosebirdClient.isDone()) {
                String msg = null;
                String key = "empty key";
                try {
                    msg = msgQueue.take();
                    producer.publish(key, msg);
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                    LOG.error("Could not consume twit", ie);
                }
            }

            latch.countDown();

        }).start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            hosebirdClient.stop();
            producer.teardown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOG.info("Application is terminated");

        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new TwitConsumer().run();
    }
}
