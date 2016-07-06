package ch.heia.pop.yarn.example;

import popjava.PopJava;
import popjava.annotation.POPClass;
import popjava.annotation.POPSyncSeq;
import popjava.system.POPSystem;

/**
 *
 * @author Dosky
 */
public class NoSpec {
   
    public static void main(String[] args) {
        POPSystem.initialize(args);
        
        System.out.println(PopJava.newActive(AAA.class).aaa());
        System.out.println(PopJava.newActive(AAA.class).aaa());
        System.out.println(PopJava.newActive(AAA.class).aaa());
        System.out.println(PopJava.newActive(AAA.class).aaa());
        System.out.println(PopJava.newActive(AAA.class).aaa());
        
    }

    
    @POPClass
    public static class AAA {

        static int aAa = 1000;
        
        public AAA() {
        }
        
        @POPSyncSeq
        public int aaa() {
            return aAa++;
        }
    }
    
}
