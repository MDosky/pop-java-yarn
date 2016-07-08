package popjava.yarn;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import popjava.service.POPJavaDeamon;
import popjava.system.POPSystem;
import popjava.yarn.command.ContainerClient;

/**
 *
 * @author Dosky
 */
public class DaemonService {
    
    private POPJavaDeamon daemon;
    
    @Parameter(names = "-pwd", required = true)
    private String password;
    @Parameter(names = "-port", required = true)
    private int port;
    @Parameter(names = "-master", required = true)
    private String masterHost;

    public static void main(String[] args) throws IOException {
        //POPSystem.setStarted();
        DaemonService mainService = new DaemonService();
        new JCommander(mainService, args);
        mainService.start();
    }

    private DaemonService() {
    }

    private void start() {
        daemon = new POPJavaDeamon(password, port);

        // start daemon thread
        Thread thread = new Thread(daemon);
        thread.setName("POP-Java Daemon");
        
        thread.start();
    }

    public void stop() {
        try {
            daemon.close();
        } catch (IOException ex) {
        }
    }
}
