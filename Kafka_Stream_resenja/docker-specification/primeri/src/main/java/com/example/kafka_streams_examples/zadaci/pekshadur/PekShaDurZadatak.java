package com.example.kafka_streams_examples.zadaci.pekshadur;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.StringJoiner;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import com.example.kafka_streams_examples.util.KafkaConstants;
import com.example.kafka_streams_examples.util.KafkaStreamsUtil;

// Zadatak 1: svaku vrednost mapirati na broj leta, dužinu leta i cenu,
// pa mapirane vrednosti slati na novi topik pek-sha-dur
public class PekShaDurZadatak {
    private static final DateTimeFormatter DATE_FORMATER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        final Properties streamsConfiguration = KafkaStreamsUtil.getStreamsConfiguration("pek-sha-dur");

        final StreamsBuilder builder = new StreamsBuilder();

        // ključ = ID, vrednost = kolone spojene sa # (0 flightNumber, 5 departureDate,
        // 6 arrivalDate, 9 price)
        final KStream<Long, String> flights = builder.stream(KafkaConstants.TOPIC_NAME);

        flights.mapValues(new ValueMapper<String, String>() {
            @Override
            public String apply(String value) {
                String[] words = value.split(KafkaConstants.SEPARATOR);

                LocalDateTime departureDateTime = LocalDateTime.parse(words[5], DATE_FORMATER); // departureDate
                LocalDateTime arrivDateTime = LocalDateTime.parse(words[6], DATE_FORMATER); // arrivalDate
                long minutes = Duration.between(departureDateTime, arrivDateTime).toMinutes();

                StringJoiner joiner = new StringJoiner(KafkaConstants.SEPARATOR);

                joiner.add(words[0]); // flightNumber - broj leta (npr. HO1252)
                joiner.add(String.valueOf(minutes));
                joiner.add(words[9]); // price - cena karte (npr. 1860)

                return joiner.toString();
            }
        }).peek((key, value) -> System.out.println("IZLAZ: " + value)).to(KafkaConstants.TOPIC_NAME + "-dur");

        @SuppressWarnings("resource")
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
