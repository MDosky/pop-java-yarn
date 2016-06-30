package ch.heia.pop.yarn.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Dosky
 */
public class DateClient {
    private final static Log LOG = LogFactory.getLog(DateClient.class);
     /**
     * Runs the client as an application.  First it displays a dialog
     * box asking for the IP address or hostname of a host running
     * the date server, then connects to it and displays the date that
     * it serves.
     */
    public static void main(String[] args) throws IOException {
        
        LOG.info("Server address " + args[0]);
        String serverAddress = args[0];
        Socket s = new Socket(serverAddress, 9090);
        BufferedReader input =
            new BufferedReader(new InputStreamReader(s.getInputStream()));
        String answer = input.readLine();
        
        LOG.info("DateServer answer " + answer);
        
        System.exit(0);
    }
}
