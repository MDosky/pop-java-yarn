package popjava.yarn;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

/**
 *
 * @author Dosky
 */
public class ApplicationMasterRMCallback implements AMRMClientAsync.CallbackHandler {

    private Container mainContainer = null;
    private int lauchedContainers;
    private int allocatedContainers;

    private final String hdfs_dir;
    private final int askedContainers;
    private final String main;
    private final List<String> args;
    private String taskServer;
    private String jobManager;
    private int numContainersToWaitFor = -1;

    private NMClient nmClient;

    public ApplicationMasterRMCallback(NMClient nmClient, String hdfs_dir, int askedContainers, String main, List<String> args) {
        this.nmClient = nmClient;
        this.hdfs_dir = hdfs_dir;
        this.askedContainers = askedContainers;
        this.main = main;
        this.args = args;
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        synchronized (this) {
            if (numContainersToWaitFor == -1)
                numContainersToWaitFor = 0;
            numContainersToWaitFor += containers.size();
        }
        // look for last container for main
        for (Container cont : containers) {
            if (++allocatedContainers == askedContainers) {
                mainContainer = cont;
            }
        }

        for (Container container : containers) {

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

            System.out.println("[AM] Starting client");
            // Launch container by create ContainerLaunchContext
            ContainerLaunchContext ctx
                    = Records.newRecord(ContainerLaunchContext.class);
            List script = Lists.newArrayList(
                    "hdfs dfs -copyToLocal " + hdfs_dir + "/pop-app.jar"
                    + ";",
                    "hdfs dfs -copyToLocal " + hdfs_dir + "/popjava.jar"
                    + ";",
                    "sleep 3"
                    + ";",
                    "$JAVA_HOME/bin/java"
                    + " -javaagent:popjava.jar"
                    + " -cp popjava.jar:pop-app.jar"
                    + " popjava.yarn.YARNContainer"
                    + " -taskServer " + taskServer
                    + " -jobmanager " + jobManager
                    + " " + mainStarter
                    + " 1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                    + " 2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                    + ";"
            );
            System.out.println("[AM] Executing: " + Arrays.toString(script.toArray(new String[0])));
            ctx.setCommands(script);

            System.out.println("[AM] Launching container " + container.getId());
            try {
                nmClient.startContainer(container, ctx);
                lauchedContainers++;
            } catch (YarnException | IOException ex) {
                System.err.println("[AM] Error launching container " + container.getId() + " " + ex);
            }
        }
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            System.out.println("[AM] Completed container " + status.getContainerId());
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
        if(lauchedContainers == 0)
            return 0f;
        return askedContainers / (float) lauchedContainers;
    }

    public boolean doneWithContainers() {
        synchronized (this) {
            return numContainersToWaitFor == 0;
        }
    }
}
