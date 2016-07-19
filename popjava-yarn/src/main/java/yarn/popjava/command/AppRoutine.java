package yarn.popjava.command;

import popjava.PopJava;
import popjava.baseobject.POPAccessPoint;
import popjava.jobmanager.ServiceConnector;
import popjava.system.POPSystem;

/**
 *
 * @author Dosky
 */
public class AppRoutine {
    
    public final TaskServer server;

    public AppRoutine(String taskAP) {
        this.server = PopJava.newActive(TaskServer.class, new POPAccessPoint(taskAP));
    }
    
    public void registerService(ServiceConnector di) {
        server.registerService(di);
    }
    
    public void waitAndQuit() {
        int killStatus = -1;
        POPAppStatus status = null;
        while(true) {
            try {
                status = server.getStatus();
                if(status != null && status.isKill()) {
                    switch(status) {
                        case FINISHED:
                            killStatus = 0;
                            break;
                        case FAILED:
                            killStatus = 11;
                            break;
                        case KILLED:
                            killStatus = 50;
                            break;
                    }

                    if(killStatus != -1) {
                        POPSystem.end();
                        System.exit(killStatus);
                    }
                }
            } catch(IllegalArgumentException ex) {
                System.err.println("Failed to get Status, continue...");
                System.err.println(ex.getMessage());
                ex.printStackTrace();
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
        }
    }
    
    public void running() {
        server.setStatus(POPAppStatus.RUNNING);
    }

    public void finish() {
        server.setStatus(POPAppStatus.FINISHED);
    }

    public void fail() {
        server.setStatus(POPAppStatus.FAILED);
    }
}
