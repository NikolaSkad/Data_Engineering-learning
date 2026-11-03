// paket = folder u kom je fajl (zadaci/pekshanight), mora da se poklapa sa putanjom
package com.example.kafka_streams_examples.zadaci.pekshanight;

import java.time.LocalDateTime;
// pretvara tekst u datum i nazad po zadatom formatu
import java.time.format.DateTimeFormatter;
// mapa podešavanja (ključ = vrednost) koju Kafka Streams čita pri pokretanju
import java.util.Properties;

// sama Kafka Streams aplikacija: pokreće topologiju i obrađuje poruke
import org.apache.kafka.streams.KafkaStreams;
// pravi topologiju (graf obrade: odakle čita, šta radi, gde piše)
import org.apache.kafka.streams.StreamsBuilder;
// tok zapisa (ključ, vrednost) koji se čita iz topika
import org.apache.kafka.streams.kstream.KStream;

// TOPIC_NAME (pek-sha, iz env-a) i SEPARATOR (#)
import com.example.kafka_streams_examples.util.KafkaConstants;
// gotova podešavanja: ime aplikacije, adresa brokera, serde za ključ (Long) i vrednost (String)
import com.example.kafka_streams_examples.util.KafkaStreamsUtil;

// Zadatak 2: filtrirati vrednosti koje imaju vreme poletanja između ponoći (0AM) i 7 ujutro (7AM),
// pa ih slati na novi topik pek-sha-night
public class PekShaNightZadatak {
    // format datuma u podacima, npr. 2019-01-04 06:35:00
    // static final = napravi se jednom za celu klasu, ne za svaku poruku
    private static final DateTimeFormatter DATE_FORMATER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // ulazna tačka programa, ovo pokreće "java -cp ... PekShaNightZadatak"
    public static void main(String[] args) {
        // podešavanja aplikacije; "pek-sha-night" je deo imena aplikacije (application.id)
        // Kafka po tom imenu pamti dokle je aplikacija stigla (offset)
        final Properties streamsConfiguration = KafkaStreamsUtil.getStreamsConfiguration("pek-sha-night");

        // prazna topologija, u nju se dodaju koraci obrade
        final StreamsBuilder builder = new StreamsBuilder();

        // source: čita topik pek-sha kao tok
        // ključ = ID, vrednost = kolone spojene sa # (5 departureDate)
        final KStream<Long, String> flights = builder.stream(KafkaConstants.TOPIC_NAME);

        flights.filter((key, value) -> {
            String[] words = value.split(KafkaConstants.SEPARATOR);
            LocalDateTime departureDateTime = LocalDateTime.parse(words[5], DATE_FORMATER); // departureDate
            int departureHour = departureDateTime.getHour();

            return departureHour < 7;
        }).peek((key, value) -> System.out.println("IZLAZ: " + value)).to("pek-sha-night");

        // SuppressWarnings: utišava upozorenje da streams nikad nije zatvoren (zatvara ga shutdown hook dole)
        @SuppressWarnings("resource")
        // builder.build() vraća Topology: opis koraka obrade (stream -> filter -> to)
        // new KafkaStreams(...) pravi objekat koji izvršava tu topologiju sa ovim podešavanjima
        // samo pravi objekat, obrada kreće tek na streams.start()
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        // briše lokalno stanje od prethodnog pokretanja (ne briše topike ni offset)
        streams.cleanUp();
        // pokreće obradu u pozadinskim nitima; main se ovde završava, a aplikacija radi dalje
        streams.start();

        // na Ctrl + C (SIGTERM) uredno zatvori aplikaciju pre gašenja
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
