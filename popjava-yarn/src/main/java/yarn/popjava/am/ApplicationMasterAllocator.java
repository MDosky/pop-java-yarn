package yarn.popjava.am;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import popjava.PopJava;
import popjava.annotation.POPClass;
import popjava.annotation.POPObjectDescription;
import popjava.annotation.POPSyncConc;
import popjava.annotation.POPSyncSeq;
import popjava.baseobject.POPAccessPoint;
import popjava.dataswaper.ObjectDescriptionInput;
import popjava.jobmanager.ResourceAllocator;
import popjava.jobmanager.ServiceConnector;

/**
 * Implementation of a ResourceAllocator.
 * This class need a POP Application Master Channel to be set to work.
 * The Channel is meanly a rely to the AM which need a lot of the Hadoop library
 * to work, by using a channel in the middle we don't need to include those
 * libraries in our classpath, making the application a lot more lightweight.
 * @author Dosky
 */
@POPClass
public class ApplicationMasterAllocator implements ResourceAllocator {

    private Queue<ServiceConnector> services;

    private final AtomicInteger currentHost = new AtomicInteger();

    private final Semaphore await = new Semaphore(0, true);
    
    private ApplicationMasterChannel channel;

    @POPObjectDescription(url = "localhost")
    public ApplicationMasterAllocator() {
        services = new ConcurrentLinkedQueue<>();
    }
    
    @POPSyncSeq
    public void setChannel(POPAccessPoint pap) {
        channel = PopJava.newActive(ApplicationMasterChannel.class, pap);
    }

    /**
     * Return the next host to use. Right now it's a Round-robin but in future
     * it could change
     *
     * @param odi
     * @return
     */
    @Override
    @POPSyncSeq
    public ServiceConnector getNextHost(ObjectDescriptionInput odi) {
        System.out.println("[JMA] Request incoming");
        try {
            // out of bound, ask for more resources
            if (currentHost.get() >= services.size()) {
                System.out.println("[JMA] Requesting new container");
                // write request in channel to AM
                channel.requestContainer((int) odi.getMemoryReq(), (int) odi.getMemoryReq());
            }
            await.acquire();
            System.out.println("[JMA] Resource is available");
        } catch (InterruptedException ex) {
        }

        // linear allocation
        currentHost.getAndIncrement();
        return services.poll();
    }

    /**
     * Register a service for maybe latter use
     * @param service 
     */
    @Override
    @POPSyncConc
    public void registerService(ServiceConnector service) {
        System.out.println("[JMA] Adding service " + service);
        services.add(service);
        // add to counter
        await.release();
    }
}
