package yarn.popjava.am;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    
    private final Map<Resource, List<AMRMClient.ContainerRequest>> resourcesRequests = new HashMap<>();
    
    private final Semaphore mutex = new Semaphore(1, true);

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
        AMRMClient.ContainerRequest[] canStart = new AMRMClient.ContainerRequest[containers.size()];
        
        // keep track of all containers
        if (numContainersToWaitFor.get() == -1)
            numContainersToWaitFor.set(containers.size());
        else
            numContainersToWaitFor.getAndAdd(containers.size());
        
        // this lot is done because of this
        // https://issues.apache.org/jira/browse/YARN-1902
        // extra containers are arriving so we have to close them
        try {
            mutex.acquire();

            // look for last container for main
            for (int i = 0; i < containers.size(); i++) {
                Container cont = containers.get(i);
                Resource res = cont.getResource();
                
                if(resourcesRequests.containsKey(res) && !resourcesRequests.get(res).isEmpty()) {
                    // set to start
                    canStart[i] = resourcesRequests.get(res).get(0);
                    
                    // remove from requests
                    resourcesRequests.get(res).remove(0);
                    
                    // look for main
                    if (allocatedContainers.incrementAndGet() == askedContainers) {
                        mainContainer = cont;
                    }
                }
            }
            
            mutex.release();
        } catch(InterruptedException e) {}
        
        for (int i = 0; i < containers.size(); i++) {
            Container container = containers.get(i);
            
            // stop extra containers before they start, why are they even here?!
            if(canStart[i] == null) {
                rmClient.releaseAssignedContainer(container.getId());
                continue;
            }
            
            // remove request from client, it's allocated now
            rmClient.removeContainerRequest(canStart[i]);
            
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
                    // copy temp dir
                      "hdfs dfs -copyToLocal " + hdfs_dir + "/*"
                    + ";",
                    // create in case no script is present
                      "touch premain.sh postmain.sh"
                    + ";",
                    // make executable
                      "chmod +x premain.sh postmain.sh"
                    + ";",
                    // execute premain user script
                      "./premain.sh " + container.getId().getContainerId()
                    + ";",
                    // init container
                      "$JAVA_HOME/bin/java"
                    + " -javaagent:popjava.jar"
                    + " -cp popjava.jar:pop-app.jar"
                    + " " + YARNContainer.class.getName()
                    + " -taskServer " + taskServer
                    + " -jobmanager " + jobManager
                    + " " + mainStarter
                    + " 1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                    + " 2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                    + ";",
                    // execute postmain user script
                      "./postmain.sh " + container.getId().getContainerId()
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

    void addToResourceRequestPool(AMRMClient.ContainerRequest request) {
        try {
            mutex.acquire();
            if(!resourcesRequests.containsKey(request.getCapability()))
                resourcesRequests.put(request.getCapability(), new ArrayList<>());
            resourcesRequests.get(request.getCapability()).add(request);
            mutex.release();
        } catch (InterruptedException ex) { }
    }

    void setRMClient(AMRMClientAsync<AMRMClient.ContainerRequest> rmClient) {
        this.rmClient = rmClient;
    }
}
