// paket = folder u kom je fajl (zadaci/letovipodanu), mora da se poklapa sa putanjom
package com.example.kafka_streams_examples.zadaci.letovipodanu;

// mapa podešavanja (ključ = vrednost) koju Kafka Streams čita pri pokretanju
import java.util.Properties;

// serde = serializer/deserializer, kaže Kafki kako da ključ i vrednost pretvori u bajtove i nazad
import org.apache.kafka.common.serialization.Serdes;
// sama Kafka Streams aplikacija: pokreće topologiju i obrađuje poruke
import org.apache.kafka.streams.KafkaStreams;
// pravi topologiju (graf obrade: odakle čita, šta radi, gde piše)
import org.apache.kafka.streams.StreamsBuilder;
// serde za groupBy, potreban jer groupBy menja tip ključa (Long ID -> String datum)
import org.apache.kafka.streams.kstream.Grouped;
// tok zapisa (ključ, vrednost) koji se čita iz topika
import org.apache.kafka.streams.kstream.KStream;

// TOPIC_NAME (pek-sha, iz env-a) i SEPARATOR (#)
import com.example.kafka_streams_examples.util.KafkaConstants;
// gotova podešavanja: ime aplikacije, adresa brokera, serde za ključ (Long) i vrednost (String)
import com.example.kafka_streams_examples.util.KafkaStreamsUtil;

// Zadatak 3: prebrojati broj letova po danu i štampati rezultate na System.out
public class LetoviPoDanuZadatak {

    // ulazna tačka programa, ovo pokreće "java -cp ... LetoviPoDanuZadatak"
    public static void main(String[] args) {
        // podešavanja aplikacije; "letovi-po-danu" je deo imena aplikacije (application.id)
        // Kafka po tom imenu pamti dokle je aplikacija stigla (offset)
        final Properties streamsConfiguration = KafkaStreamsUtil.getStreamsConfiguration("letovi-po-danu");

        // prazna topologija, u nju se dodaju koraci obrade
        final StreamsBuilder builder = new StreamsBuilder();

        // source: čita topik pek-sha kao tok
        // ključ = ID, vrednost = kolone spojene sa # (5 departureDate, npr. 2019-01-04 06:35:00)
        final KStream<Long, String> flights = builder.stream(KafkaConstants.TOPIC_NAME);

        flights.groupBy((key, value) -> {
            String[] words = value.split(KafkaConstants.SEPARATOR);
            String departureDay = words[5].substring(0, 10);

            return departureDay;
        }, Grouped.with(Serdes.String(), Serdes.String())).count().toStream()
                .foreach((day, count) -> System.out.println(day + " -> " + count));
        ;

        // SuppressWarnings: utišava upozorenje da streams nikad nije zatvoren (zatvara ga shutdown hook dole)
        @SuppressWarnings("resource")
        // builder.build() vraća Topology: opis koraka obrade (stream -> groupBy -> count -> foreach)
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
