/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yarn.popjava;

import popjava.PopJava;
import popjava.annotation.POPClass;
import popjava.jobmanager.POPJavaJobManager;
import popjava.system.POPSystem;
import popjava.util.Util;
import yarn.popjava.command.AppRoutine;
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
        
        jobManager = PopJava.newActive(POPJavaJobManager.class);
        POPSystem.jobService = jobManager.getAccessPoint();
        taskServer = PopJava.newActive(TaskServer.class, jobManager);
        
        // server status, waiting
        taskServer.setStatus(POPAppStatus.WAITING);
        
        // printout to share
        System.out.println(TASK + taskServer.getAccessPoint());
        System.out.println(JOBM + jobManager.getAccessPoint());
        
        AppRoutine appRoutine = new AppRoutine(taskServer.getAccessPoint().toString());
        
        while(!appRoutine.server.getStatus().isKill()) {
            Thread.sleep(1000);
        }
        Thread.sleep(10000);
        POPSystem.end();
    }
}
