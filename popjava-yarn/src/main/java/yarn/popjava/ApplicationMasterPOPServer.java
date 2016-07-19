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
    public static final String TASK = "TASK_SERVER_AP=";
    public static final String JOBM = "JOBM_SERVER_AP=";
    
    public static void main(String[] args) throws InterruptedException {
        
        TaskServer taskServer;
        POPJavaJobManager jobManager;
        
        System.out.println("[POPServer] Starting servers");
        jobManager = PopJava.newActive(POPJavaJobManager.class);
        POPSystem.jobService = jobManager.getAccessPoint();
        taskServer = PopJava.newActive(TaskServer.class, jobManager);
        System.out.println("[POPServer] Done");
        
        System.out.println("[POPServer] Setting App Status as ACCEPTED");
        // server status, waiting
        taskServer.setStatus(POPAppStatus.ACCEPTED);
        
        System.out.println("[POPServer] Setting servers addresses in AppMaster");
        // set server by using known strings
        System.out.println(TASK + taskServer.getAccessPoint());
        System.out.println(JOBM + jobManager.getAccessPoint());
        
        System.out.println("[POPServer] Addresses set");
        
        System.out.println("[POPServer] Waiting for application to die");
        while(!taskServer.getStatus().isKill()) {
            Thread.sleep(1000);
        }
        Thread.sleep(10000);
    }
}
