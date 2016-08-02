package yarn.popjava.command;

import popjava.annotation.POPAsyncConc;
import popjava.annotation.POPClass;
import popjava.annotation.POPObjectDescription;
import popjava.annotation.POPSyncConc;
import popjava.jobmanager.POPJavaJobManager;
import popjava.jobmanager.ServiceConnector;

/**
 * Offer method to set or see the status of the application.
 * It's also the link to the POPJavaJobManager so we can easily register
 * ServiceConnectors to it.
 * @author Dosky
 */
@POPClass
public class TaskServer {
    
    private POPAppStatus status = POPAppStatus.WAITING;
    private POPJavaJobManager jm;

    @POPObjectDescription(url = "localhost")
    public TaskServer() {
    }

    @POPObjectDescription(url = "localhost")
    public TaskServer(POPJavaJobManager jm) {
        this.jm = jm;
    }
    
    @POPAsyncConc
    public void registerService(ServiceConnector di) {
        jm.registerService(di);
        System.out.println("[TS] Registering service " + di);
    }
    
    @POPAsyncConc
    public void setStatus(POPAppStatus status) {
        this.status = status;
    }
    
    @POPSyncConc
    public POPAppStatus getStatus() {
        return status;
    }
}
