package testsuite.barrier;

import java.io.IOException;
import java.util.Date;

import popjava.PopJava;
import popjava.annotation.POPClass;
import popjava.baseobject.POPAccessPoint;

@POPClass(isDistributable = false)
public class MainBarrier {

    public static void main(String... argvs) throws IOException, InterruptedException {
        System.out.println("Barrier: Starting test...");

        int nbWorkers = 12;
        if (argvs.length > 0) {
            nbWorkers = Integer.parseInt(argvs[0]);
        }

        System.out.println(new Date());
        Barrier b = new Barrier(nbWorkers);
        Worker[] pa = new Worker[nbWorkers];
        for (int i = 0; i < nbWorkers; i++) {
            Worker w = new Worker();
            pa[i] = w;
            w.setNo(i);
            w.work(b);
        }

        System.out.println(new Date());
        //Give time to worker to finish their job
        int timeout = 120;
        int count;
        while((count = b.getCurrentCounter()) != 0 && timeout-- > 0) {
            System.out.println(String.format("Counter at %s, Timeout at %d", count, timeout));
            Thread.sleep(1000);
        }
        System.out.println(new Date());

        for (int i = 0; i < pa.length; i++) {
            Worker w = pa[i];
            if (w.getNo() != i + 10) {
                System.out.println("Barrier Test failed");
                return;
            }
        }
        System.out.println("Barrier test successful");

    }
}
