package popjava.yarn;

import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Random;
import popjava.service.DaemonInfo;
import popjava.service.POPJavaDeamon;
import popjava.system.POPSystem;
import popjava.yarn.command.AppRoutine;

/**
 * Start the POP-Java daemon
 * @author Dosky
 */
public class DaemonService {
    
    private POPJavaDeamon daemon;
    private DaemonInfo di;
    
    private static final Random rnd = new SecureRandom();

    /**
     * Start a standalone java daemon
     * @param args The first parameter should be the string value of a DaemonInfo
     */
    public static void main(String... args) {
        DaemonService mainService = new DaemonService();
        mainService.start();
        
        POPSystem.initialize(args);
        // server address
        AppRoutine appRoutine = new AppRoutine(args[0]);
        appRoutine.registerDaemon(mainService.di.toString());
        appRoutine.waitAndQuit();
    }

    private DaemonService() {
        di = new DaemonInfo(generatePassword());
    }

    private void start() {
        daemon = new POPJavaDeamon(di);

        // start daemon thread
        Thread thread = new Thread(daemon);
        thread.setName("POP-Java Daemon");
        
        thread.start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {}
        System.out.println("Started POP Daemon on " + di.toString());
    }

    public void stop() {
        try {
            daemon.close();
        } catch (IOException ex) {
        }
    }
    
    private static String generatePassword() {
        return new BigInteger(256, rnd).toString(Character.MAX_RADIX);
    }
}
