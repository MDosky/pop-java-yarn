package yarn.popjava.command;

import popjava.PopJava;
import popjava.annotation.POPClass;
import popjava.annotation.POPObjectDescription;
import popjava.annotation.POPParameter;
import popjava.annotation.POPSyncSeq;
import popjava.base.POPObject;
import popjava.baseobject.POPAccessPoint;
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
    
    @POPSyncSeq
    public void setJobManager(POPAccessPoint pap) {
        jm = PopJava.newActive(POPJavaJobManager.class, pap);
    }
    
    @POPSyncSeq
    public void registerDaemon(ServiceConnector di) {
        jm.registerService(di);
        System.out.println("[TS] Registering service " + di);
    }
    
    @POPSyncSeq
    public void setStatus(POPAppStatus status) {
        this.status = status;
    }
    
    @POPSyncSeq
    public POPAppStatus getStatus() {
        return status;
    }
}
