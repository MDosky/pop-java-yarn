package popjava.yarn.command;

import popjava.PopJava;
import popjava.baseobject.POPAccessPoint;
import popjava.service.DaemonInfo;
import popjava.system.POPSystem;

/**
 *
 * @author Dosky
 */
public class AppRoutine {
    
    private final TaskServer server;

    public AppRoutine(String taskAP) {
        this.server = PopJava.newActive(TaskServer.class, new POPAccessPoint(taskAP));
    }
    
    public void registerDaemon(String di) {
        System.out.println("AppRoutine registerDaemon " + di);
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
                        killStatus = 100;
                        break;
                }
                
                if(killStatus != -1) {
                    POPSystem.end();
                    System.exit(killStatus);
                }
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException ex) {
            }
        }
    }

    public void finish() {
        server.setStatus(POPAppStatus.FINISHED);
    }

    public void fail() {
        server.setStatus(POPAppStatus.FAILED);
    }
}
