package yarn.popjava.command;

import popjava.PopJava;
import popjava.annotation.POPClass;
import popjava.annotation.POPObjectDescription;
import popjava.annotation.POPParameter;
import popjava.annotation.POPSyncConc;
import popjava.annotation.POPSyncMutex;
import popjava.annotation.POPSyncSeq;
import popjava.base.POPObject;
import popjava.jobmanager.POPJavaJobManager;
import popjava.jobmanager.ServiceConnector;
import popjava.service.DaemonInfo;

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
    public void registerDaemon(@POPParameter(POPParameter.Direction.INOUT) DaemonInfo di) {
        jm.registerService(di);
        System.out.println("[TS] Registering service " + di);
    }
    
    @POPSyncMutex
    public void setStatus(POPAppStatus status) {
        this.status = status;
    }
    
    @POPSyncConc
    public POPAppStatus getStatus() {
        try {
            // try value, if not send wait
            POPAppStatus as = POPAppStatus.valueOf(status.name());
            return status;
        } catch(IllegalArgumentException e) {
            return POPAppStatus.WAITING;
        }
    }
}
