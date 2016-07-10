package ch.heia.pop.yarn.example;

import java.util.Arrays;
import popjava.PopJava;
import popjava.annotation.POPAsyncConc;
import popjava.annotation.POPClass;
import popjava.base.POPObject;
import popjava.system.POPSystem;

/**
 *
 * @author Dosky
 */
public class NoSpec {
   
    public static void main(String[] args) throws InterruptedException {
        System.out.println(POPSystem.jobService);
        POPSystem.initialize(args);
        System.out.println(POPSystem.jobService);
        System.out.println(Arrays.toString(args));
        
        //POPJavaJobManager popJm = PopJava.newActive(POPJavaJobManager.class);
        
        AAA aaa;
        
        aaa = PopJava.newActive(AAA.class);
        aaa.aaa();
        aaa.exit();
        Thread.sleep(5000);
        aaa = PopJava.newActive(AAA.class);
        aaa.aaa();
        aaa.exit();
        Thread.sleep(5000);
        aaa = PopJava.newActive(AAA.class);
        aaa.aaa();
        aaa.exit();
        Thread.sleep(5000);
        aaa = PopJava.newActive(AAA.class);
        aaa.aaa();
        aaa.exit();
        Thread.sleep(5000);
        aaa = PopJava.newActive(AAA.class);
        aaa.aaa();
        aaa.exit();
        
        POPSystem.end();
    }

    
    @POPClass
    public static class AAA extends POPObject {

        static int aAa = 1000;
        
        public AAA() {
        }
        
        @POPAsyncConc
        public void aaa() {
            System.out.println(System.currentTimeMillis());
        }
    }
    
}
