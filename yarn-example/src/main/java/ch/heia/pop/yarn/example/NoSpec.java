package ch.heia.pop.yarn.example;

import popjava.PopJava;
import popjava.annotation.POPAsyncConc;
import popjava.annotation.POPClass;
import popjava.annotation.POPConfig;
import popjava.annotation.POPSyncSeq;
import popjava.base.POPObject;
import popjava.service.POPJavaJobManager;
import popjava.system.POPSystem;

/**
 *
 * @author Dosky
 */
public class NoSpec {
   
    public static void main(String[] args) {
        POPSystem.initialize(args);
        
        //POPJavaJobManager popJm = PopJava.newActive(POPJavaJobManager.class);
        
        System.out.println(PopJava.newActive(AAA.class).aaa());
        System.out.println(PopJava.newActive(AAA.class).aaa());
        System.out.println(PopJava.newActive(AAA.class).aaa());
        System.out.println(PopJava.newActive(AAA.class).aaa());
        System.out.println(PopJava.newActive(AAA.class).aaa());
        
        POPSystem.end();
    }

    
    @POPClass
    public static class AAA extends POPObject {

        static int aAa = 1000;
        
        public AAA() {
        }
        
        @POPAsyncConc
        public long aaa() {
            return aAa + System.currentTimeMillis();
        }
    }
    
}
