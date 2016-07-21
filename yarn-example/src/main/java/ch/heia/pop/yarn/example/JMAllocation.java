package ch.heia.pop.yarn.example;

import java.util.Date;
import popjava.annotation.POPClass;
import popjava.annotation.POPSyncSeq;

/**
 *
 * @author Dosky
 */
@POPClass(isDistributable = false)
public class JMAllocation {

    public static void main(String[] args) {
        System.out.println("Starting pop java app");
        
        TestClass instance;

        System.out.println("First");
        instance = new TestClass();
        report(instance.getMillis());

        System.out.println("Second");
        instance = new TestClass();
        report(instance.getMillis());

        System.out.println("Third");
        instance = new TestClass();
        report(instance.getMillis());

        System.out.println("Forth");
        instance = new TestClass();
        report(instance.getMillis());

        System.out.println("Fifth");
        instance = new TestClass();
        report(instance.getMillis());

        System.out.println("End app");
    }
    
    private static void report(long millis) {
        System.out.println("Remote Obj reported: " + new Date(millis));
        System.out.println("Server time is     : " + new Date());
        System.out.println();
    }

    @POPClass
    public static class TestClass {

        public TestClass() {
        }

        @POPSyncSeq
        public long getMillis() {
            return System.currentTimeMillis();
        }
    }

}
