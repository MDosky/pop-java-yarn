package ch.heia.pop.yarn.example;

import java.util.Arrays;
import popjava.PopJava;
import popjava.annotation.POPAsyncConc;
import popjava.annotation.POPClass;
import popjava.annotation.POPSyncSeq;
import popjava.base.POPObject;
import popjava.system.POPSystem;

/**
 *
 * @author Dosky
 */
public class NoSpec {
   
    public static void main(String[] args) throws InterruptedException {
        System.out.println(POPSystem.jobService);
        System.out.println("9 " + POPSystem.initialized);
        POPSystem.initialize(args);
        System.out.println("10" + POPSystem.initialized);
        System.out.println(POPSystem.jobService);
        
        //POPJavaJobManager popJm = PopJava.newActive(POPJavaJobManager.class);
        
        System.out.println("Starting pop java app");
        AAA aaa;
        
        System.out.println("first");
        aaa = PopJava.newActive(AAA.class);
        System.out.println(aaa.aaa());
        aaa.exit();
        Thread.sleep(5000);
        System.out.println("second");
        aaa = PopJava.newActive(AAA.class);
        System.out.println(aaa.aaa());
        aaa.exit();
        Thread.sleep(5000);
        System.out.println("third");
        aaa = PopJava.newActive(AAA.class);
        System.out.println(aaa.aaa());
        aaa.exit();
        Thread.sleep(5000);
        System.out.println("forth");
        aaa = PopJava.newActive(AAA.class);
        System.out.println(aaa.aaa());
        aaa.exit();
        Thread.sleep(5000);
        System.out.println("fifth");
        aaa = PopJava.newActive(AAA.class);
        System.out.println(aaa.aaa());
        aaa.exit();
        
        System.out.println("end app");
        POPSystem.end();
    }

    
    @POPClass
    public static class AAA extends POPObject {

        static int aAa = 1000;
        
        public AAA() {
        }
        
        @POPSyncSeq
        public long aaa() {
            return System.currentTimeMillis();
        }
    }
    
}
