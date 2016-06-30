package ch.heia.pop.yarn.app;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * This class implements a simple async app master. In real usages, the
 * callbacks should execute in a separate thread or thread pool
 */
public class ApplicationMasterAsync implements AMRMClientAsync.CallbackHandler {

    Configuration configuration;
    NMClient nmClient;

    private int numContainersToWaitFor;

    @Parameter(names = "--dir", required = true)
    private String hdfs_dir;
    private String jar_name;
    @Parameter(names = "--vcores", required = true)
    private int vcores;
    @Parameter(names = "--memory", required = true)
    private int memory;
    @Parameter(names = "--containers", required = true)
    private int containers;
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
        configuration = new YarnConfiguration();
        nmClient = NMClient.createNMClient();
        nmClient.init(configuration);
        nmClient.start();
    }

    private void setup() {
        numContainersToWaitFor = containers;
        jar_name = new File(hdfs_dir).getName();
    }

    private Container mainContainer = null;

    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            // skip main if it appears again
            if (mayStartMainApp(container)) {
                continue;
            }

            System.out.println("[AM] Starting client");
            // Launch container by create ContainerLaunchContext
            ContainerLaunchContext ctx
                    = Records.newRecord(ContainerLaunchContext.class);
            ctx.setCommands(
                    Lists.newArrayList(
                            "sleep 10"
                            + ";",
                            "echo " + container.getNodeId().getHost()
                            + " 1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                            + " 2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                            + ";",
                            "ip addr"
                            + " 1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                            + " 2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                            + ";",
                            //"hdfs dfs -copyToLocal " + hdfs_dir + " " + jar_name
                            //+ " 1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                            //+ " 2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                            //+ ";",
                            "$JAVA_HOME/bin/java ch.heia.popdna.myapp.DateClient " //+ main + " " + args
                            + " 1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                            + " 2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                            + ";"
                    ));
            LocalResource clientJar = Records.newRecord(LocalResource.class);
            try {
                setupClientJar(new Path(hdfs_dir + "/pop-app.jar"), clientJar);
            } catch (IOException ex) {}
            ctx.setLocalResources(
                    Collections.singletonMap("pop-app.jar", clientJar));
            System.out.println("[AM] Launching container " + container.getId());
            try {
                nmClient.startContainer(container, ctx);
            } catch (Exception ex) {
                System.err.println("[AM] Error launching container " + container.getId() + " " + ex);
            }
        }
    }

    private boolean mayStartMainApp(Container container) {
        if (mainContainer == null || container == mainContainer) {
            System.out.println("[AM] Starting server");

            // keep track of container
            mainContainer = container;

            // Launch container by create ContainerLaunchContext
            ContainerLaunchContext ctx
                    = Records.newRecord(ContainerLaunchContext.class);
            ctx.setCommands(
                    Lists.newArrayList(
                            "echo " + container.getNodeId().getHost()
                            + " 1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                            + " 2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                            + ";",
                            "date -R"
                            + " 1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                            + " 2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                            + ";",
                            //"hdfs dfs -copyToLocal " + hdfs_dir + " " + jar_name
                            //+ " 1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                            //+ " 2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                            //+ ";",
                            "$JAVA_HOME/bin/java ch.heia.popdna.myapp.DateServer "
                            + //args +
                            " 1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                            + " 2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                            + ";"
                    ));
            System.out.println("[AM] Launching container " + container.getId());
            try {
                nmClient.startContainer(container, ctx);
            } catch (Exception ex) {
                System.err.println("[AM] Error launching container " + container.getId() + " " + ex);
            }

            return true;
        }
        return false;
    }

    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            System.out.println("[AM] Completed container " + status.getContainerId());
            synchronized (this) {
                numContainersToWaitFor--;
            }
        }
    }

    public void onNodesUpdated(List<NodeReport> updated) {
    }

    public void onReboot() {
    }

    public void onShutdownRequest() {
    }

    public void onError(Throwable t) {
    }

    public float getProgress() {
        return 0;//1f - containers / (float) numContainersToWaitFor;
    }

    public boolean doneWithContainers() {
        return numContainersToWaitFor == 0;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void runMainLoop() throws Exception {
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
        for (int i = 0; i < containers; i++) {
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
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
        System.out.println("[AM] unregisterApplicationMaster 1");
    }

    private void setupClientJar(org.apache.hadoop.fs.Path jarPath, LocalResource clientJar) throws IOException {
        FileStatus jarStat = FileSystem.get(configuration).getFileStatus(jarPath);
        clientJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        clientJar.setSize(jarStat.getLen());
        clientJar.setTimestamp(jarStat.getModificationTime());
        clientJar.setType(LocalResourceType.FILE);
        clientJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }

}
