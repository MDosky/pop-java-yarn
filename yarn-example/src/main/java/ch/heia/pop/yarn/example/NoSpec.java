package ch.heia.pop.yarn.example;

import java.util.logging.Level;
import java.util.logging.Logger;
import popjava.annotation.POPClass;
import popjava.annotation.POPSyncSeq;
import popjava.system.POPSystem;

/**
 *
 * @author Dosky
 */
@POPClass(isDistributable = false)
public class NoSpec {

    public static void main(String[] args) {
        try {
            System.out.println("Starting pop java app");
            AAA aaa;

            System.out.println("first");
            aaa = new AAA();
            System.out.println(aaa.aaa());
            Thread.sleep(5000);
            
            System.out.println("second");
            aaa = new AAA();
            System.out.println(aaa.aaa());
            Thread.sleep(5000);
            
            System.out.println("third");
            aaa = new AAA();
            System.out.println(aaa.aaa());
            Thread.sleep(5000);
            
            System.out.println("forth");
            aaa = new AAA();
            System.out.println(aaa.aaa());
            Thread.sleep(5000);
            
            System.out.println("fifth");
            aaa = new AAA();
            System.out.println(aaa.aaa());

            System.out.println("end app");
        } catch (InterruptedException ex) {
            Logger.getLogger(NoSpec.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @POPClass
    public static class AAA {

        static int aAa = 1000;

        public AAA() {
        }

        @POPSyncSeq
        public long aaa() {
            System.out.println("aaa write aaa");
            return System.currentTimeMillis();
        }
    }

}
