package yarn.popjava.command;

import popjava.PopJava;
import popjava.baseobject.POPAccessPoint;
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
    
    public void registerDaemon(String di) {
        server.registerDaemon(di);
    }
    
    public void waitAndQuit() {
        int killStatus = -1;
        while(true) {
            POPAppStatus status = server.getStatus();
            if(status.isKill()) {
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
