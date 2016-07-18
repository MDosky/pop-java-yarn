package yarn.popjava.command;

import popjava.annotation.POPClass;
import popjava.annotation.POPObjectDescription;
import popjava.annotation.POPSyncConc;
import popjava.annotation.POPSyncMutex;
import popjava.base.POPObject;
import popjava.jobmanager.POPJavaJobManager;

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
    
    @POPSyncMutex
    public void registerDaemon(String di) {
        jm.registerDaemon(di);
        System.out.println("[TS] Registering service " + di);
    }
    
    @POPSyncMutex
    public void setStatus(POPAppStatus status) {
        this.status = status;
    }
    
    @POPSyncConc
    public POPAppStatus getStatus() {
        return status;
    }
}
