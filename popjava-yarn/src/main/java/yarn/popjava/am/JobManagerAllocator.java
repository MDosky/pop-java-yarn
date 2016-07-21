package yarn.popjava.am;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import popjava.PopJava;
import popjava.annotation.POPClass;
import popjava.annotation.POPObjectDescription;
import popjava.annotation.POPSyncConc;
import popjava.annotation.POPSyncSeq;
import popjava.base.POPObject;
import popjava.baseobject.POPAccessPoint;
import popjava.dataswaper.ObjectDescriptionInput;
import popjava.jobmanager.ResourceAllocator;
import popjava.jobmanager.ServiceConnector;

/**
 *
 * @author Dosky
 */
@POPClass
public class JobManagerAllocator implements ResourceAllocator {

    private List<ServiceConnector> services;

    private final AtomicInteger currentHost = new AtomicInteger();

    private final Semaphore await = new Semaphore(0, true);
    
    private ApplicationMasterChannel channel;

    @POPObjectDescription(url = "localhost")
    public JobManagerAllocator() {
        services = new LinkedList<>();
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
        System.out.println("[JMA] Request in");
        try {
            // out of bound, reset
            if (currentHost.get() >= services.size()) {
                System.out.println("[JMA] Requesting new container");
                // write request in channel to AM
                channel.requestContainer((int) odi.getMemoryReq(), (int) odi.getMemoryReq());
            }
            await.acquire();
            System.out.println("[JMA] Can acquire resource");
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
    @POPSyncConc
    public void registerService(ServiceConnector service) {
        System.out.println("[JMA] Adding service " + service);
        services.add(service);
        // add to counter
        await.release();
    }
}
