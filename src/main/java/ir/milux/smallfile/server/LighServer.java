package ir.milux.smallfile.server;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

public class LighServer {
    private Server server;

    public LighServer(int port) {

        ResourceConfig config = new ResourceConfig();
        config.packages("ir.milux.smallfile.server");
        ServletHolder servlet = new ServletHolder(new ServletContainer(config));

        server = new Server(port);
        ServletContextHandler context = new ServletContextHandler(server,"/*");
        context.addServlet(servlet,"/*");
    }

    public void start() throws Exception {
        server.start();
        server.join();
    }

    public void stop() {
        server.destroy();
    }
}