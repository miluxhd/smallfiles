package ir.milux.smallfile.client;

import com.datastax.driver.core.*;
import com.rabbitmq.client.*;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.nio.ByteBuffer;
public class RocConsumer {
    private static Logger logger = Logger.getRootLogger();
    public static void main(String[] args) throws IOException, InterruptedException {
        final String QUEUE_NAME = "cassandra-input-bin";
        ConnectionFactory factory = new ConnectionFactory();
        factory.setPassword("milux");
        factory.setUsername("milux");
        factory.setHost("inf-rmq");
        Channel inputChannel = null;

        String keyspace = "smallfiles";
        String hosts = "cassandra-1,cassandra-2,cassandra-3,cassandra-4,cassandra-5,cassandra-6,cassandra-7,cassandra-8,cassandra-9,cassandra-10,cassandra-11";
        Cluster.Builder cluster = Cluster.builder();
        for (String s :
                hosts.split(",")) {
            cluster.addContactPoint(s);
        }
        final Session session = cluster.build().connect(keyspace);

        final PreparedStatement preparedStatement = session.prepare("INSERT INTO SMALLFILES.IMAGES(address, binary) VALUES (?, ?)");
        final FinalBatchStatement customFinalBatchStatement = new FinalBatchStatement(200,150);

        try {
            inputChannel = factory.newConnection().createChannel();
            inputChannel.queueDeclare(QUEUE_NAME,true,false,false,null);
        } catch (Exception e) {
            logger.error(RocConsumer.class+","+e);
        }

        inputChannel.basicQos(1000);
        Channel finalInputChannel = inputChannel;
        final Consumer consumer = new DefaultConsumer(finalInputChannel) {
            @Override
            public void handleDelivery(String consumerTag,Envelope envelope,AMQP.BasicProperties properties,byte[] body) throws IOException {
                RocFile rocFile = RocFile.deserialize(body);
                BoundStatement statement = preparedStatement.bind(rocFile.getName(),ByteBuffer.wrap(rocFile.serialize()));
                customFinalBatchStatement.add(statement);
                if (customFinalBatchStatement.getBatchSize() >= customFinalBatchStatement.getMaxBatchSize()) {
                    try {
                        session.execute(customFinalBatchStatement.getStatement());
                    } catch (Exception e) {
                        customFinalBatchStatement.executeSequential(session);
                        logger.error(RocConsumer.class+","+e);
                    }

                    logger.info(RocConsumer.class+", Inserted Rows : " + customFinalBatchStatement.getBatchSize());
                    customFinalBatchStatement.reset();
                    finalInputChannel.basicAck(envelope.getDeliveryTag(),true);

                    if (customFinalBatchStatement.getBatchSize() <= customFinalBatchStatement.getMaxBatchSize()) {
                        logger.info(RocConsumer.class+", Increasing batchsize size : " + (customFinalBatchStatement.getBatchSize()));
                        customFinalBatchStatement.increaseBatchSize(1);
                    }
                }
            }
        };
        boolean autoAck = false;
        inputChannel.basicConsume(QUEUE_NAME,autoAck,consumer);

    }
}

