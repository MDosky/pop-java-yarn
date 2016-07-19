package yarn.popjava.command;

import popjava.annotation.POPAsyncConc;
import popjava.annotation.POPClass;
import popjava.annotation.POPObjectDescription;
import popjava.annotation.POPSyncConc;
import popjava.base.POPObject;
import popjava.jobmanager.POPJavaJobManager;
import popjava.jobmanager.ServiceConnector;

/**
 *
 * @author Dosky
 */
@POPClass
public class TaskServer extends POPObject {
    
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
