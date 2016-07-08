package popjava.yarn;

import java.io.IOException;
import popjava.service.DaemonInfo;
import popjava.service.POPJavaDeamon;

/**
 * Start the POP-Java daemon
 * @author Dosky
 */
public class DaemonService {
    
    private POPJavaDeamon daemon;
    private DaemonInfo di;

    /**
     * Start a standalone java daemon
     * @param args The first parameter should be the string value of a DaemonInfo
     */
    public static void main(String[] args) {
        //POPSystem.setStarted();
        DaemonService mainService = new DaemonService(args);
        mainService.start();
    }

    private DaemonService(String... args) {
        di = new DaemonInfo(args[0]);
    }

    private void start() {
        daemon = new POPJavaDeamon(di.getPassword(), di.getPort());

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
