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
        POPSystem.initialize(args);
        
        //POPJavaJobManager popJm = PopJava.newActive(POPJavaJobManager.class);
        
        AAA aaa;
        
        aaa = PopJava.newActive(AAA.class);
        System.out.println(aaa.aaa());
        aaa.exit();
        Thread.sleep(5000);
        aaa = PopJava.newActive(AAA.class);
        System.out.println(aaa.aaa());
        aaa.exit();
        Thread.sleep(5000);
        aaa = PopJava.newActive(AAA.class);
        System.out.println(aaa.aaa());
        aaa.exit();
        Thread.sleep(5000);
        aaa = PopJava.newActive(AAA.class);
        System.out.println(aaa.aaa());
        aaa.exit();
        Thread.sleep(5000);
        aaa = PopJava.newActive(AAA.class);
        System.out.println(aaa.aaa());
        aaa.exit();
        
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
