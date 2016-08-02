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

        MyAllocator instance;

        int n;
        try {
            n = Integer.parseInt(args[0]);
        } catch (Exception e) {
            n = 20;
        }

        for (int i = 0; i < n; i++) {
            System.out.println("Run " + i);
            instance = new MyAllocator();
            report(instance.getMillis(), instance.getContainer());
        }

        System.out.println("End app");
    }

    private static void report(long millis, String container) {
        System.out.println("Remote Obj reported: " + new Date(millis));
        System.out.println("Server time is     : " + new Date());
        System.out.println("Container is       : " + container);
        System.out.println();
    }

    @POPClass
    public static class MyAllocator {

        public MyAllocator() {
        }

        @POPSyncSeq
        public long getMillis() {
            return System.currentTimeMillis();
        }

        @POPSyncSeq
        public String getContainer() {
            return System.getenv("CONTAINER_ID");
        }
    }

}
