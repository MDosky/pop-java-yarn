package yarn.popjava;

import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Random;
import popjava.baseobject.ConnectionType;
import popjava.baseobject.POPAccessPoint;
import popjava.jobmanager.ServiceConnector;
import popjava.service.POPJavaDeamon;
import popjava.system.POPSystem;
import yarn.popjava.command.AppRoutine;

/**
 * Start the POP-Java daemon
 * @author Dosky
 */
public class DaemonService {
    
    private POPJavaDeamon daemon;
    private ServiceConnector di;
    
    private static final Random rnd = new SecureRandom();

    /**
     * Start a standalone java daemon
     * @param args The first parameter should be the string value of a DaemonInfo
     */
    public static void main(String... args) {
        if(args.length != 2)
            throw new IllegalArgumentException("Expected: TaskServer AP, JM AP");
        
        POPSystem.jobService = new POPAccessPoint(args[1]);
        
        DaemonService mainService = new DaemonService();
        mainService.start();
        // server address
        AppRoutine appRoutine = new AppRoutine(args[0]);
        appRoutine.registerService(mainService.di);
        appRoutine.waitAndQuit();
    }

    private DaemonService() {
        di = new ServiceConnector(generatePassword(), 0, ConnectionType.DEAMON);
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
        System.out.println("[DM] Started POP Daemon on " + di.toString());
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
