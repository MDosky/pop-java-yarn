package yarn.popjava;

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
public class JobManagerAllocator extends POPObject implements ResourceAllocator {

    private final List<ServiceConnector> services;

    private final AtomicInteger currentHost = new AtomicInteger();

    private final Semaphore await = new Semaphore(0, true);
    
    private final ApplicationMasterChannel channel;

//    public static final String MSG_ALLOC = "[JMC] alloc";

    @POPObjectDescription(url = "localhost")
    public JobManagerAllocator() {
        services = null;
        channel = null;
    }
    
    @POPObjectDescription(url = "localhost")
    public JobManagerAllocator(String channelPapString) {
        services = new LinkedList<>();
        channel = PopJava.newActive(ApplicationMasterChannel.class, new POPAccessPoint(channelPapString));
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
        try {
            // out of bound, reset
            if (currentHost.get() >= services.size()) {
                // write request in channel to AM
                channel.requestContainer((int) odi.getMemoryReq(), (int) odi.getMemoryReq());
            }
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
    @POPSyncConc
    public void registerService(ServiceConnector service) {
        services.add(service);
        // add to counter
        await.release();
    }
}
