package ir.milux.smallfile.server;

import org.apache.log4j.Logger;


public class RunServer {
    private static Logger logger = Logger.getRootLogger();
    public static void main(String[] args) throws Exception {
        int port = 7268;
        LighServer lighServer = new LighServer(port);
        logger.info("starting http server on port: "+port);
        lighServer.start();
    }
}
