package testsuite.barrier;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import popjava.annotation.POPClass;
import popjava.annotation.POPSyncConc;

@POPClass
public class Barrier {

    protected final AtomicInteger counter;
    protected final Lock lock = new ReentrantLock();
    protected final Condition event = lock.newCondition();
    protected final double rnd = Math.random();

    public Barrier() throws IOException {
        this(15);
    }

    public Barrier(int n) throws IOException {
        try (BufferedWriter out = new BufferedWriter(new FileWriter("/tmp/barrier" + rnd, true))) {
            counter = new AtomicInteger(n);
            out.write("Barrier closed for " + counter + "\n");
        }
        System.out.println("The barrier is closed for " + counter + " workers");
    }

    @POPSyncConc
    public void activate() throws InterruptedException, IOException {
        lock.lock();
        try {
            //TODO: Find Bugs throws an error in this method. the lock is not always unlocked in all codepaths
            BufferedWriter out = new BufferedWriter(new FileWriter("/tmp/barrier" + rnd, true));
            counter.decrementAndGet();
            out.write("Counter = " + counter + "\n");
            if (counter.get() == 0) {
                out.write("Barrier open\n");
                out.close();
                event.signalAll();
            } else {
                out.write("Wait\n");
                out.close();
                event.await();//TODO: Should be in a loop
            }
        } finally {
            lock.unlock();
        }
    }

}
