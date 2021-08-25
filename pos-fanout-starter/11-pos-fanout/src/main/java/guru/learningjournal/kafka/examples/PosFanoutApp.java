package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class PosFanoutApp {

    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        //REQUIREMENT ONE//

        //1.Configuring our Stream Properties.
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,AppConfigs.applicationID);
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,AppConfigs.bootstrapServers);

        //2.Create a source stream
        //This is our Source Processor KS0 according to our DAG
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, PosInvoice> KS0 = streamsBuilder.stream(
                AppConfigs.posTopicName,
                Consumed.with(AppSerdes.String(),AppSerdes.PosInvoice()));
        //3.Apply filter and whatever is the resulting stream,send it to Kafka Topic.
        KS0.filter((k, v) ->
                v.getDeliveryType().equalsIgnoreCase(AppConfigs.DELIVERY_TYPE_HOME_DELIVERY))
                .to(AppConfigs.shipmentTopicName,Produced.with(AppSerdes.String(),AppSerdes.PosInvoice()));

        //REQUIREMENT TWO//
        KS0.filter((k, v) ->
                v.getCustomerType().equalsIgnoreCase(AppConfigs.CUSTOMER_TYPE_PRIME))
                //Transform the value into a notification
                .mapValues(invoice -> RecordBuilder.getNotification(invoice))
                .to(AppConfigs.notificationTopic, Produced.with(AppSerdes.String(),AppSerdes.Notification()));

        //REQUIREMENT THREE//
        //Transform the Invoice into a masked invoice
        KS0.mapValues(invoice -> RecordBuilder.getMaskedInvoice(invoice))
                .flatMapValues(invoice -> RecordBuilder.getHadoopRecords(invoice))
                .to(AppConfigs.hadoopTopic, Produced.with(AppSerdes.String(),AppSerdes.HadoopRecord()));

        //ALL OUR OPERATIONS FOR THE TOPOLOGY ARE DEFINED
        //NOW WE CREATE A KAFKA STREAM AND START IT
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(),properties);
        streams.start();

        //Add a Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Stream");
            streams.close();
        }));
    }
}
