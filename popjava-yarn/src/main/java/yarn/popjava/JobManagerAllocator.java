package yarn.popjava;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import popjava.dataswaper.ObjectDescriptionInput;
import popjava.jobmanager.ResourceAllocator;
import popjava.jobmanager.ServiceConnector;

/**
 *
 * @author Dosky
 */
public class JobManagerAllocator implements ResourceAllocator {

    private final List<ServiceConnector> services;

    private final AtomicInteger currentHost = new AtomicInteger();

    private final Semaphore await = new Semaphore(0, true);
    private final Semaphore sync = new Semaphore(1, true);

    public static final String MSG_ALLOC = "[JMC] alloc";

    public JobManagerAllocator() {
        this.services = new LinkedList<>();
    }

    /**
     * Return the next host to use. Right now it's a Round-robin but in future
     * it could change
     *
     * @param odi
     * @return
     */
    @Override
    public ServiceConnector getNextHost(ObjectDescriptionInput odi) {
        try {
            sync.acquire();
            // out of bound, reset
            if (currentHost.get() >= services.size()) {
                // write request to stdout, AM will catch it
                System.out.println(String.format(MSG_ALLOC + " %f %f", odi.getMemoryReq(), odi.getMemoryReq()));
            }
            sync.release();
            await.acquire();
        } catch (InterruptedException ex) {
        }

        // linear allocation
        return services.get(currentHost.getAndIncrement());
    }

    /**
     * Register a service for maybe latter use
     * @param service 
     */
    @Override
    public void registerService(ServiceConnector service) {
        services.add(service);
        // add to counter
        await.release();
    }
}
