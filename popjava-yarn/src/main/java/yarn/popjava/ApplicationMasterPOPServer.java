/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yarn.popjava;

import popjava.PopJava;
import popjava.annotation.POPClass;
import popjava.baseobject.POPAccessPoint;
import popjava.jobmanager.POPJavaJobManager;
import popjava.system.POPSystem;
import yarn.popjava.command.POPAppStatus;
import yarn.popjava.command.TaskServer;

/**
 *
 * @author Dosky
 */
@POPClass(isDistributable = false)
public class ApplicationMasterPOPServer {
    
    public static void main(String[] args) throws InterruptedException {
        ApplicationMasterPOP amp = PopJava.newActive(ApplicationMasterPOP.class, new POPAccessPoint(args[0]));
        
        TaskServer taskServer;
        POPJavaJobManager jobManager;
        
        System.out.println("[POPServer] Starting servers");
        jobManager = new POPJavaJobManager();
        POPSystem.jobService = jobManager.getAccessPoint();
        taskServer = new TaskServer(jobManager);
        System.out.println("[POPServer] Done");
        
        System.out.println("[POPServer] Setting App Status as ACCEPTED");
        // server status, waiting
        taskServer.setStatus(POPAppStatus.ACCEPTED);
        System.out.println("[POPServer] Status Changed");
        
        System.out.println("[POPServer] Setting servers addresses in AppMaster");
        // set server
        amp.setServer(taskServer.getAccessPoint().toString(), jobManager.getAccessPoint().toString());
        System.out.println("[POPServer] Addresses set");
        
        System.out.println("[POPServer] Waiting for application to die");
        while(!taskServer.getStatus().isKill()) {
            Thread.sleep(1000);
        }
        Thread.sleep(10000);
    }
}
