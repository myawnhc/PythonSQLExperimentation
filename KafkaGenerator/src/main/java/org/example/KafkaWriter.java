package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.GeneratorFunction;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

public class KafkaWriter {

    public static final int ITEMS_PER_SECOND = 1; // generation rate

    public static Pipeline createPipeline() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", StringSerializer.class.getCanonicalName());
        props.setProperty("value.serializer", StringSerializer.class.getCanonicalName());

        // Variable needs to be final, so wrap as an array so we can increment the content
        final int[] sequence = new int[] {0};

        Pipeline p = Pipeline.create();
        StreamStage<String> items = p.readFrom(
                TestSources.itemStream(ITEMS_PER_SECOND, new EventGenerator()))
                .withNativeTimestamps(0);

        // Make a key-value pair from the generated items
        StreamStage<Map.Entry<String,String>> entries = items.map(item -> {
            String key = String.valueOf(sequence[0]++);
            return Tuple2.tuple2(key, item);
        });

        entries.writeTo(KafkaSinks.kafka(props)
                .topic("Topic-A")
                .extractKeyFn(entry -> ((Map.Entry)entry).getKey())
                .extractValueFn(entry -> ((Map.Entry)entry).getValue())
                .build());

        entries.writeTo(KafkaSinks.kafka(props)
                .topic("Topic-B")
                .extractKeyFn(entry -> ((Map.Entry)entry).getKey())
                .extractValueFn(entry -> ((Map.Entry)entry).getValue())
                .build());

        entries.writeTo(KafkaSinks.kafka(props)
                .topic("Topic-C")
                .extractKeyFn(entry -> ((Map.Entry)entry).getKey())
                .extractValueFn(entry -> ((Map.Entry)entry).getValue())
                .build());

        entries.writeTo(Sinks.logger());

        return p;
    }
    public static void main(String[] args) {

        // Configuration for testing in embedded mode
        Config hzConfig = new Config();
        hzConfig.getJetConfig().setEnabled(true);
        hzConfig.getJetConfig().setResourceUploadEnabled(true);
//        hzConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
//        hzConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
//        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(hzConfig);

        // No non-default client configuration needed when running in client/server mode
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(hzConfig);

        JetService jet = hazelcast.getJet();
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Kafka Item Generator");
        jobConfig.addClass(KafkaWriter.class);
        jobConfig.addClass(EventGenerator.class);
        Job job = jet.newJobIfAbsent(createPipeline(), jobConfig);
        job.join();
    }
}