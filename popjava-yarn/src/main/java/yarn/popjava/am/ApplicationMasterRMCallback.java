package yarn.popjava.am;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import yarn.popjava.YARNContainer;

/**
 * This class is called by the RS when new containers are allocated and
 * to know the overall status of the application.
 * @author Dosky
 */
public class ApplicationMasterRMCallback implements AMRMClientAsync.CallbackHandler {

    private Container mainContainer = null;
    private final AtomicInteger lauchedContainers = new AtomicInteger();
    private final AtomicInteger allocatedContainers = new AtomicInteger();
    private final AtomicInteger numContainersToWaitFor = new AtomicInteger(-1);
    
    private AMRMClientAsync<AMRMClient.ContainerRequest> rmClient;

    private final String hdfs_dir;
    private final int askedContainers;
    private final String main;
    private final List<String> args;
    private String taskServer;
    private String jobManager;

    private final NMClient nmClient;
    
    private List<Resource> resourcesRequests = new ArrayList<>();
    
    private Semaphore mutex = new Semaphore(1);

    /**
     * Some information from the main class, where things are located and
     * whatnot.
     * @param nmClient
     * @param hdfs_dir
     * @param askedContainers
     * @param main
     * @param args 
     */
    public ApplicationMasterRMCallback(NMClient nmClient, String hdfs_dir, int askedContainers, String main, List<String> args) {
        this.nmClient = nmClient;
        this.hdfs_dir = hdfs_dir;
        this.askedContainers = askedContainers;
        this.main = main;
        this.args = args;
    }
    
    @Override
    public void onContainersAllocated(List<Container> containers) {
        // continue list
        boolean[] canStart = new boolean[containers.size()];
        
        try {
            mutex.acquire();

            // look for last container for main
            for (int i = 0; i < containers.size(); i++) {
                Container cont = containers.get(i);
                
                if(resourcesRequests.contains(cont.getResource())) {
                    // set to start
                    canStart[i] = true;
                    
                    // remove from requests
                    resourcesRequests.remove(cont.getResource());
                    
                    // look for main
                    if (allocatedContainers.incrementAndGet() == askedContainers) {
                        mainContainer = cont;
                    }
                    
                    // increment to see the end
                    if (numContainersToWaitFor.get() == -1)
                        numContainersToWaitFor.set(1);
                    else
                        numContainersToWaitFor.incrementAndGet();
                }
            }
            
            mutex.release();
        } catch(InterruptedException e) {}
        
        for (int i = 0; i < containers.size(); i++) {
            Container container = containers.get(i);
            
            // stop extra containers before they start, why are they even here?!
            if(!canStart[i]) {
                rmClient.releaseAssignedContainer(container.getId());
                continue;
            }
            
            String mainStarter = "";
            // master container, who will start the main
            if (container == mainContainer) {
                String argsString = "";
                for (String s : args) {
                    argsString += s + " ";
                }
                
                mainStarter = " -main "
                        + " -mainClass " + main + " " + argsString;
            }

            System.out.println("[RM] Starting client");
            // Launch container by create ContainerLaunchContext
            ContainerLaunchContext ctx
                    = Records.newRecord(ContainerLaunchContext.class);
            List script = Lists.newArrayList(
                    "hdfs dfs -copyToLocal " + hdfs_dir + "/pop-app.jar"
                    + ";",
                    "hdfs dfs -copyToLocal " + hdfs_dir + "/popjava.jar"
                    + ";",
                    "$JAVA_HOME/bin/java"
                    + " -javaagent:popjava.jar"
                    + " -cp popjava.jar:pop-app.jar"
                    + " " + YARNContainer.class.getName()
                    + " -taskServer " + taskServer
                    + " -jobmanager " + jobManager
                    + " " + mainStarter
                    + " 1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                    + " 2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                    + ";"
            );
            System.out.println("[RM] Executing: " + Arrays.toString(script.toArray(new String[0])));
            ctx.setCommands(script);

            System.out.println("[RM] Launching container " + container.getId());
            try {
                nmClient.startContainer(container, ctx);
                lauchedContainers.incrementAndGet();
            } catch (YarnException | IOException ex) {
                System.err.println("[RM] Error launching container " + container.getId() + " " + ex);
            }
        }
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            System.out.println("[RM] Completed container " + status.getContainerId());
            numContainersToWaitFor.decrementAndGet();
        }
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updated) {
    }

    @Override
    public void onShutdownRequest() {
    }

    @Override
    public void onError(Throwable t) {
        t.printStackTrace();
    }

    @Override
    public float getProgress() {
        if(lauchedContainers.get() >= askedContainers)
            return 1f;
        return lauchedContainers.get() / (float) askedContainers;
    }

    public boolean doneWithContainers() {
        return numContainersToWaitFor.get() == 0;
    }

    void setServer(String taskServer, String jobManager) {
        this.taskServer = taskServer;
        this.jobManager = jobManager;
    }

    void addToResourceRequestPool(Resource capability) {
        try {
            mutex.acquire();
            resourcesRequests.add(capability);
            mutex.release();
        } catch (InterruptedException ex) { }
    }

    void setRMClient(AMRMClientAsync<AMRMClient.ContainerRequest> rmClient) {
        this.rmClient = rmClient;
    }
}
