/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package popjava.yarn;

import popjava.PopJava;
import popjava.jobmanager.POPJavaJobManager;
import popjava.system.POPSystem;
import popjava.yarn.command.AppRoutine;
import popjava.yarn.command.POPAppStatus;
import popjava.yarn.command.TaskServer;

/**
 *
 * @author Dosky
 */
public class ApplicationMasterPOPServer {
    public static void main(String[] args) throws InterruptedException {
        TaskServer taskServer;
        POPJavaJobManager jobManager;
        
        POPSystem.initialize();
        jobManager = PopJava.newActive(POPJavaJobManager.class);
        POPSystem.jobService = jobManager.getAccessPoint();
        taskServer = PopJava.newActive(TaskServer.class);
        
        taskServer.setJobManager(jobManager.getAccessPoint());
        // server status, waiting
        taskServer.setStatus(POPAppStatus.WAITING);
        
        System.out.println(taskServer.getAccessPoint());
        System.out.println(jobManager.getAccessPoint());
        
        AppRoutine appRoutine = new AppRoutine(taskServer.getAccessPoint().toString());
        
        while(!appRoutine.server.getStatus().isKill())
            Thread.sleep(1000);
        Thread.sleep(10000);
        POPSystem.end();
    }
}
