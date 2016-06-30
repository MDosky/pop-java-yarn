package ch.heia.pop.yarn.example;

import java.math.BigInteger;
import java.net.SocketException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import popjava.PopJava;
import popjava.system.POPSystem;

/**
 *
 * @author Dosky
 */
public class Main {
    private static String[] addresses = new String[] {
        "localhost", "198.128.209.55", "198.128.210.80"
    };
    private static int n;
        
    public static void main(String[] args) throws InterruptedException, SocketException {        
        POPSystem.initialize(args);
        
        System.out.println("Arguments:");
        for(String s : args) {
            System.out.println("  " + s);
        }
        
        try {
            n = Integer.parseInt(args[2]);
        } catch(Exception e) {
            System.err.println("Missing second parameter.");
            System.exit(1);
        }
        
        addresses = Arrays.copyOfRange(args, 3, args.length);
        
        if(args.length > 1) {
            if("1".equals(args[1])) {
                first(args);
            }
            else if("2".equals(args[1])) {
                second(args);
            }
        }
        
        POPSystem.end();
    }

    private static void first(String... args) throws InterruptedException {
        int m = addresses.length;
        
        Random rnd = new SecureRandom();
        
        TestClass[] objs = new TestClass[n];
        
        for(int i = 0; i < n; i++)
            objs[i] = PopJava.newActive(TestClass.class, addresses[rnd.nextInt(m)]);
        
        System.out.println("Starting doing something...");
        for (TestClass obj : objs)
            obj.doSomething();
        
        System.out.println("Told to do something concurrent?\nWaiting...");
        
        Thread.sleep(5000);
        
        for (TestClass obj : objs) {
            System.out.println("Report for " + obj);
            System.out.println(obj.report());
        }
    }

    private static void second(String... args) throws SocketException {
        int m = addresses.length;
        Random rnd = new SecureRandom();
        String[] sendTo = new String[n];
        for(int i = 0; i < n; i++)
            sendTo[i] = addresses[rnd.nextInt(m)];
        
        First f = PopJava.newActive(First.class, addresses[0]);
        f.dos(sendTo);
    }
}
