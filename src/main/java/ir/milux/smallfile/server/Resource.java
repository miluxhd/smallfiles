package ir.milux.smallfile.server;
import com.datastax.driver.core.*;
import ir.milux.smallfile.client.RocFile;
import org.apache.log4j.Logger;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.nio.ByteBuffer;

@Path("/")
public class Resource extends  ResourceConfig{
    private static final Session session = getSession();
    private static Logger logger = Logger.getRootLogger();
    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public byte[] getIt(@QueryParam("q") String q) {
        logger.info(Resource.class+", Get request: "+q);
        PreparedStatement preparedStatement = null;
        preparedStatement = session.prepare("select binary from smallfiles.images where address=?");
        BoundStatement statement = new BoundStatement(preparedStatement);
        statement.bind(q);
        ResultSet results = session.execute(statement);
        Row a = results.one();
        ByteBuffer imageAsByte = a.getBytes("binary");
        RocFile rocFile = RocFile.deserialize(imageAsByte.array());
        System.out.println(imageAsByte.array().length);
        return rocFile.getBinary();
    }

    public static Session getSession() {
        Session session = null;

            String hostname = "cassandra-1,cassandra-2,cassandra-3,cassandra-4,cassandra-5,cassandra-6,cassandra-7,cassandra-8";
            String keyspace = "smallfiles";
            Cluster.Builder cluster = Cluster.builder();
            for (String s :
                    hostname.split(",")) {
                cluster.addContactPoint(s);
            }
        session = cluster.build().connect(keyspace);
        return session;
    }
}