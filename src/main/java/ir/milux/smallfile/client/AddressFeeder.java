package ir.milux.smallfile.client;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import org.apache.log4j.Logger;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class AddressFeeder {
    private static Logger logger = Logger.getRootLogger();
    public static void main(String[] args) throws IOException {

        final String QUEUE_NAME = "cassandra-input-address";
        ConnectionFactory factory = new ConnectionFactory();
        factory.setPassword("username");
        factory.setUsername("password");
        factory.setHost("rabbit");
        Channel channel = null;

        try {
            channel = factory.newConnection().createChannel();
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        }catch (Exception e){
            logger.error(AddressFeeder.class+","+e);
        }
        String line;
        BufferedReader reader = new BufferedReader(new FileReader("local_file_address_list"));
        while ((line = reader.readLine())!=null){
            channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_BASIC, line.getBytes());
        }

        try {
            channel.close();
        } catch (TimeoutException e) {
            logger.error(e);
        }
    }
}

