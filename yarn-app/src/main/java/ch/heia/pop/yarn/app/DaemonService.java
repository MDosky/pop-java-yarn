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
    }

    public DaemonService() {
    }
    
        
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
