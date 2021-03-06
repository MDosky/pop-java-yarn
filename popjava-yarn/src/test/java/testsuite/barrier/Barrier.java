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
import popjava.annotation.POPSyncSeq;

@POPClass
public class Barrier {

    protected final AtomicInteger counter;
    protected final Lock lock = new ReentrantLock();
    protected final Condition event = lock.newCondition();
    protected final double rnd = Math.random();

    public Barrier() throws IOException {
        counter = new AtomicInteger();
        System.out.println("Init " + this.toString() + " empty with couter = " + counter.get());
    }

    public Barrier(int n) throws IOException {
        counter = new AtomicInteger(n);
        System.out.println("Init " + this.toString() + " with couter = " + counter.get());
    }

    @POPSyncConc
    public void activate() throws InterruptedException, IOException {
        System.out.println("Using " + this.toString() + " with  couter = " + counter.get());
        lock.lock();
        try {
            //TODO: Find Bugs throws an error in this method. the lock is not always unlocked in all codepaths
            counter.decrementAndGet();
            System.out.println("Counter = " + counter);
            if (counter.get() == 0) {
                System.out.println("Barrier open");
                event.signalAll();
            } else {
                System.out.println("Wait");
                event.await();//TODO: Should be in a loop
            }
        } finally {
            lock.unlock();
        }
    }
    
    @POPSyncConc
    public int getCurrentCounter() {
        return counter.get();
    }

}
