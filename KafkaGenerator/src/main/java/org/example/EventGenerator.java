package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.hazelcast.jet.pipeline.test.GeneratorFunction;

import java.time.OffsetDateTime;

public class EventGenerator implements GeneratorFunction<String> {
    public long id;
    public OffsetDateTime event_time;
    public String dataA;
    public String dataB;
    public String dataC;

    @Override
    public String generate(long timestamp, long sequence) throws Exception {
        this.id = sequence;
        this.event_time = OffsetDateTime.now();
        // timestamp format doesn't match what ODT.of() is expecting, so ignore it for now
                // OffsetDateTime.of(LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC), ZoneOffset.UTC);
        this.dataA = "A data " + id;
        this.dataB = "B data " + id;
        this.dataC = "C data " + id;

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        String json = mapper.writeValueAsString(this);
        System.out.println(json);
        return json;
    }
}
