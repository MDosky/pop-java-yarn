package popjava.yarn;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import popjava.system.POPSystem;

/**
 * This class implements a simple async app master.
 */
public class ApplicationMasterAsync implements AMRMClientAsync.CallbackHandler {

    private final Configuration configuration;
    private final NMClient nmClient;

    private int numContainersToWaitFor;
    private Container mainContainer = null;
    private int lauchedContainers;
    private int allocatedContainers;

    private Process popProcess;
    private String taskServer;
    private String jobManager;

    @Parameter(names = "--dir", required = true)
    private String hdfs_dir;
    @Parameter(names = "--vcores", required = true)
    private int vcores;
    @Parameter(names = "--memory", required = true)
    private int memory;
    @Parameter(names = "--containers", required = true)
    private int askedContainers;
    @Parameter(names = "--main", required = true)
    private String main;
    @Parameter
    private List<String> args = new ArrayList<>();

    public static void main(String... args) throws Exception {
        ApplicationMasterAsync master = new ApplicationMasterAsync();
        new JCommander(master, args);
        master.setup();
        master.runMainLoop();
    }

    public ApplicationMasterAsync() {
//        exitPassword = generatePassword();

        configuration = new YarnConfiguration();
        nmClient = NMClient.createNMClient();
        nmClient.init(configuration);
        nmClient.start();
    }

    private void setup() {
        numContainersToWaitFor = askedContainers;
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        // look for last container for main
        for (Container cont : containers) {
            if(++allocatedContainers == askedContainers)
                mainContainer = cont;
        }

        for (Container container : containers) {

            String mainStarter = "";
            // master container, who will start the main
            if (container == mainContainer) {
                mainStarter = " -main "
                        + " -mainClass " + main + " " + args;
                // server status, running
                //taskServer.setStatus(POPAppStatus.RUNNING);
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
        popProcess.destroy();
    }

    @Override
    public void onError(Throwable t) {
        t.printStackTrace();
    }

    @Override
    public float getProgress() {
        return lauchedContainers / (float) askedContainers;
    }

    public boolean doneWithContainers() {
        return numContainersToWaitFor == 0;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void runMainLoop() throws Exception {
        startCentralServers();

        AMRMClientAsync<ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(getConfiguration());
        rmClient.start();

        // Register with ResourceManager
        System.out.println("[AM] registerApplicationMaster 0");
        rmClient.registerApplicationMaster("", 0, "");
        System.out.println("[AM] registerApplicationMaster 1");

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(memory);
        capability.setVirtualCores(vcores);

        // Make container requests to ResourceManager
        for (int i = 0; i < askedContainers; i++) {
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            System.out.println("[AM] Making res-req " + i);
            rmClient.addContainerRequest(containerAsk);
        }
        
        System.out.println("[AM] waiting for containers to finish");
        while (!doneWithContainers()) {
            Thread.sleep(100);
        }

        System.out.println("[AM] unregisterApplicationMaster 0");
        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
        System.out.println("[AM] unregisterApplicationMaster 1");
        
        // quit pop java
        POPSystem.end();
    }
    
    private void startCentralServers() throws IOException {
        List<String> popServer = Lists.newArrayList(
            System.getProperty("java.home") + "/bin/java",
                "-javaagent:popjava.jar", 
                "-cp", "popjava.jar:pop-app.jar",
                "popjava.yarn.ApplicationMasterPOPServer"
        );
        
        ProcessBuilder pb = new ProcessBuilder(popServer);
        popProcess = pb.start();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(popProcess.getInputStream()))) {
            taskServer = reader.readLine();
            jobManager = reader.readLine();
        }
    }
}
