package ch.heia.pop.yarn.example;

import java.util.Date;
import popjava.annotation.POPClass;
import popjava.annotation.POPSyncSeq;

/**
 *
 * @author Dosky
 */
@POPClass(isDistributable = false)
public class NoSpec {

    public static void main(String[] args) {
        System.out.println("Starting pop java app");
        AAA aaa;

        System.out.println("first");
        aaa = new AAA();
        aaa.aaa();
        System.out.println(new Date());

        System.out.println("second");
        aaa = new AAA();
        aaa.aaa();
        System.out.println(new Date());

        System.out.println("third");
        aaa = new AAA();
        aaa.aaa();
        System.out.println(new Date());

        System.out.println("forth");
        aaa = new AAA();
        aaa.aaa();
        System.out.println(new Date());

        System.out.println("fifth");
        aaa = new AAA();
        aaa.aaa();
        System.out.println(new Date());

        System.out.println("end app");
    }

    @POPClass
    public static class AAA {

        static int aAa = 1000;

        public AAA() {
        }

        @POPSyncSeq
        public long aaa() {
            return System.currentTimeMillis();
        }
    }

}
