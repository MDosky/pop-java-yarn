/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package popjava.yarn;

import popjava.PopJava;
import popjava.jobmanager.POPJavaJobManager;
import popjava.system.POPSystem;
import popjava.util.Util;
import popjava.yarn.command.AppRoutine;
import popjava.yarn.command.POPAppStatus;
import popjava.yarn.command.TaskServer;

/**
 *
 * @author Dosky
 */
public class ApplicationMasterPOPServer {
    public static final String TASK = Util.generateRandomString(10) + "=";
    public static final String JOBM = Util.generateRandomString(10) + "=";
    
    public static void main(String[] args) throws InterruptedException {
        TaskServer taskServer;
        POPJavaJobManager jobManager;
        
        jobManager = PopJava.newActive(POPJavaJobManager.class);
        POPSystem.jobService = jobManager.getAccessPoint();
        taskServer = PopJava.newActive(TaskServer.class);
        
        taskServer.setJobManager(jobManager.getAccessPoint());
        // server status, waiting
        taskServer.setStatus(POPAppStatus.WAITING);
        
        // printout to share
        System.out.println(TASK + taskServer.getAccessPoint());
        System.out.println(JOBM + jobManager.getAccessPoint());
        
        AppRoutine appRoutine = new AppRoutine(taskServer.getAccessPoint().toString());
        
        POPJavaJobManager keepAlive = PopJava.newActive(POPJavaJobManager.class, jobManager.getAccessPoint());
        POPJavaJobManager tmpAlive;
        while(!appRoutine.server.getStatus().isKill()) {
            tmpAlive = PopJava.newActive(POPJavaJobManager.class, jobManager.getAccessPoint());
            keepAlive.exit();
            keepAlive = tmpAlive;
            Thread.sleep(1000);
        }
        keepAlive.exit();
        Thread.sleep(10000);
        POPSystem.end();
    }
}
