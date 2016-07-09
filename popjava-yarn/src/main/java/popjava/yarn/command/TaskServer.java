package popjava.yarn.command;

import popjava.PopJava;
import popjava.annotation.POPAsyncConc;
import popjava.annotation.POPAsyncMutex;
import popjava.annotation.POPClass;
import popjava.annotation.POPParameter;
import popjava.annotation.POPSyncMutex;
import popjava.annotation.POPSyncSeq;
import popjava.base.POPObject;
import popjava.baseobject.POPAccessPoint;
import popjava.jobmanager.POPJavaJobManager;
import popjava.service.DaemonInfo;

/**
 *
 * @author Dosky
 */
@POPClass
public class TaskServer extends POPObject {
    
    private POPAppStatus status = POPAppStatus.WAITING;
    private POPJavaJobManager jm;
    
    @POPSyncMutex
    public void setJobManager(POPAccessPoint pap) {
        jm = PopJava.newActive(POPJavaJobManager.class, pap);
    }
    
    @POPSyncSeq
    public void registerDaemon(@POPParameter(POPParameter.Direction.IN) DaemonInfo di) {
        jm.addDaemon(di);
    }
    
    @POPSyncMutex
    public void setStatus(@POPParameter(POPParameter.Direction.IN) POPAppStatus status) {
        this.status = status;
    }
    
    @POPSyncSeq
    public POPAppStatus getStatus() {
        return status;
    }
}
