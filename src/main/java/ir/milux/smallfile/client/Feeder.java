package ir.milux.smallfile.client;

import com.rabbitmq.client.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Feeder {
    private static Logger logger = Logger.getRootLogger();

    public static void main(String[] args) throws IOException, InterruptedException {
        final String INPUT_QUEUE_NAME = "cassandra-input-address";
        final String OUTUT_QUEUE_NAME = "cassandra-input-bin";
        ConnectionFactory factory = new ConnectionFactory();
        factory.setPassword("milux");
        factory.setUsername("milux");
        factory.setHost("inf-rmq");
        Channel inputChannel = null;
        Channel outputChannel = null;
        try {
            inputChannel = factory.newConnection().createChannel();
            outputChannel = factory.newConnection().createChannel();

            inputChannel.queueDeclare(INPUT_QUEUE_NAME, true, false, false, null);
            outputChannel.queueDeclare(OUTUT_QUEUE_NAME, true, false, false, null);
        } catch (Exception e) {
            logger.error(Feeder.class + "," + e);
        }
        inputChannel.basicQos(1);
        Channel finalInputChannel = inputChannel;
        Channel finalOutputChannel = outputChannel;
        final Consumer consumer = new DefaultConsumer(finalInputChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String filename = new String(body, "UTF-8");
                try {
                    byte[] bytes = Files.readAllBytes(Paths.get(filename));
                    System.out.println(filename);
                    RocFile roc = new RocFile(filename, bytes, filename.substring(filename.lastIndexOf(".") + 1));
                    finalOutputChannel.basicPublish("", OUTUT_QUEUE_NAME, MessageProperties.PERSISTENT_BASIC, roc.serialize());
                    finalInputChannel.basicAck(envelope.getDeliveryTag(), false);
                } catch (IOException e) {
                    logger.error(Feeder.class + "," + e);
                    finalInputChannel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };
        boolean autoAck = false;
        inputChannel.basicConsume(INPUT_QUEUE_NAME, autoAck, consumer);

    }
}

