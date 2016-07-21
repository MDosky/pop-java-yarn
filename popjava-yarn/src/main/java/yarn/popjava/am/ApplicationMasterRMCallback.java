package yarn.popjava.am;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
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

    private final String hdfs_dir;
    private final int askedContainers;
    private final String main;
    private final List<String> args;
    private String taskServer;
    private String jobManager;
    private int numContainersToWaitFor = -1;

    private final NMClient nmClient;

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
    
    List<Long> conList = new ArrayList<>();
    Map<Long, String> conMap = new HashMap<>();

    @Override
    public void onContainersAllocated(List<Container> containers) {
        synchronized (this) {
            if (numContainersToWaitFor == -1)
                numContainersToWaitFor = 0;
            numContainersToWaitFor += containers.size();
        }
        // look for last container for main
        for (Container cont : containers) {
            if (allocatedContainers.incrementAndGet() == askedContainers) {
                mainContainer = cont;
            }
        }

        for (Container container : containers) {
            
            long key = container.getId().getContainerId();
            conList.add(key);
            if(conMap.containsKey(key))
               conMap.put(key, container.getId() + " ");
            else
                conMap.put(key, conMap.get(key) + container.getId() + " ");
            
            System.out.println("[RM] Check " + conList.size() + " =? " + conMap.size());
            System.out.println("[RM] Check " + conMap.get(key));

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
            synchronized (this) {
                numContainersToWaitFor--;
            }
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
        synchronized (this) {
            return numContainersToWaitFor == 0;
        }
    }

    void setServer(String taskServer, String jobManager) {
        this.taskServer = taskServer;
        this.jobManager = jobManager;
    }
}
