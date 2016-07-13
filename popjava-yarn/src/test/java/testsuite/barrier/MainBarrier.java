package testsuite.barrier;

import java.io.IOException;

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

        Barrier b = new Barrier();
        b.setBarier(nbWorkers);
        Worker[] pa = new Worker[nbWorkers];
        for (int i = 0; i < nbWorkers; i++) {
            Worker w = new Worker();
            pa[i] = w;
            w.setNo(i);
            w.work(b);
        }

        //Give time to worker to finish their job
        Thread.sleep(20000);

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
