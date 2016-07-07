package ch.heia.pop.yarn.example;

import popjava.PopJava;
import popjava.annotation.POPAsyncConc;
import popjava.annotation.POPClass;
import popjava.annotation.POPConfig;
import popjava.annotation.POPSyncConc;
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
        
        PopJava.newActive(AAA.class).aaa();
        PopJava.newActive(AAA.class).aaa();
        PopJava.newActive(AAA.class).aaa();
        PopJava.newActive(AAA.class).aaa();
        PopJava.newActive(AAA.class).aaa();
        
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
