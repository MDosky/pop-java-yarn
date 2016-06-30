package ch.heia.pop.yarn.app;

import java.io.IOException;
import popjava.service.POPJavaDeamon;
import popjava.system.POPSystem;

/**
 *
 * @author Dosky
 */
public class DaemonService {
    
    private POPJavaDeamon daemon;

    public static void main(String[] args) {
        POPSystem.initialize(args);
        DaemonService mainService = new DaemonService(args[1], Integer.parseInt(args[2]), false);
    }

    public DaemonService() {
        this("", POPJavaDeamon.POP_JAVA_DEAMON_PORT, false);
    }
    
    public DaemonService(String password, int port, boolean isStandalone) {
        daemon = new POPJavaDeamon(password, port);
        
        Thread thread = new Thread(daemon);
        thread.setDaemon(isStandalone);
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
