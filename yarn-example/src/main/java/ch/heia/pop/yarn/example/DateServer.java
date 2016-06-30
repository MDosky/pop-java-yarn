package ch.heia.pop.yarn.example;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Dosky
 */
public class DateServer {
    
    private final static Log LOG = LogFactory.getLog(DateServer.class);

    /**
     * Runs the server.
     */
    public static void main(String[] args) throws IOException {
        
        new Thread(() -> {
            LOG.info("Starting automatic app kill");
            try {
                Thread.sleep(60000);   
            } catch (InterruptedException ex) {
                LOG.info("TIME ?!");
            }
            LOG.info("Forcing app to close...");
            System.exit(0);
        }).start();
        
        LOG.info("Starting date server");
        ServerSocket listener = new ServerSocket(9090);
        LOG.info("Waiting for connection, host " + listener.getInetAddress().getHostName());
        try {
            while (true) {
                Socket socket = listener.accept();
                LOG.info("Connection accepted for " + socket.getInetAddress().getHostAddress());
                try {
                    PrintWriter out
                            = new PrintWriter(socket.getOutputStream(), true);
                    out.println(new Date().toString());
                } finally {
                    socket.close();
                }
            }
        } finally {
            LOG.info("Closing date server");
            listener.close();
        }
    }
}
