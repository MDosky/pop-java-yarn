package popjava.yarn;

import popjava.service.DaemonInfo;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
import popjava.service.POPJavaDeamon;
import popjava.yarn.command.ContainerServer;

/**
 * This class implements a simple async app master.
 */
public class ApplicationMasterAsync implements AMRMClientAsync.CallbackHandler {

    private final Configuration configuration;
    private final NMClient nmClient;

    private int numContainersToWaitFor;
    private Container mainContainer = null;
    private int lauchedContainers;

    private final Map<Long, DaemonInfo> daemonInfo = new HashMap<>();
    private final Random rnd = new SecureRandom();
    private final String exitPassword;
    private int lastPort = POPJavaDeamon.POP_JAVA_DEAMON_PORT;

    @Parameter(names = "--master", required = true)
    private String hostname;
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
        exitPassword = generatePassword();

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
        // assign to vagabond containers
        for (Container cont : containers) {
            long id = cont.getId().getContainerId();
            if (!daemonInfo.containsKey(id)) {
                DaemonInfo di = new DaemonInfo(cont.getNodeId().getHost(), generatePassword(), ++lastPort, id);
                daemonInfo.put(id, di);

                // check for last container
                if (daemonInfo.size() == this.askedContainers) {
                    mainContainer = cont;
                }
            }
        }

        for (Container container : containers) {
            DaemonInfo di = daemonInfo.get(container.getId().getContainerId());

            String mainStarter = "";
            // master container, who will start the main
            if (container == mainContainer) {
                String daemons = "";
                for (DaemonInfo info : daemonInfo.values()) {
                    daemons += " -daemon " + info.toString();
                }

                mainStarter = " -main "
                        + " " + daemons
                        + " -mainClass " + main + " " + args;
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
                "sleep 5"
                + ";",
                "$JAVA_HOME/bin/java"
                + " -javaagent:popjava.jar"
                + " -cp popjava.jar:pop-app.jar"
                + " popjava.yarn.YARNContainer"
                + " -myDaemon " + di.toString()
                + " " + mainStarter
                + " 1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                + " 2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                + ";"
            );
            System.out.println("[AM] Executing: " + Arrays.toString(script.toArray(new String[0])));
            ctx.setCommands(script);

            //LocalResource popJar = Records.newRecord(LocalResource.class);
            //LocalResource appJar = Records.newRecord(LocalResource.class);
            //try {
            //    setupClientJar(new Path(hdfs_dir + "/pop-app.jar"), popJar);
            //    setupClientJar(new Path(hdfs_dir + "/yarn-app.jar"), popJar);
            //} catch (IOException ex) {}
            //Map<String, LocalResource> resources = new HashMap<>();
            //resources.put("pop-app.jar", popJar);
            //resources.put("yarn-app.jar", appJar);
            //ctx.setLocalResources(resources);
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
        return lauchedContainers / (float) askedContainers;
    }

    public boolean doneWithContainers() {
        return numContainersToWaitFor == 0;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void runMainLoop() throws Exception {
        startContainerListener();

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
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
        System.out.println("[AM] unregisterApplicationMaster 1");
    }

//    private void setupClientJar(org.apache.hadoop.fs.Path jarPath, LocalResource clientJar) throws IOException {
//        FileStatus jarStat = FileSystem.get(configuration).getFileStatus(jarPath);
//        clientJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
//        clientJar.setSize(jarStat.getLen());
//        clientJar.setTimestamp(jarStat.getModificationTime());
//        clientJar.setType(LocalResourceType.FILE);
//        clientJar.setVisibility(LocalResourceVisibility.PUBLIC);
//    }

    private String generatePassword() {
        return new BigInteger(256, rnd).toString(Character.MAX_RADIX);
    }

    private void startContainerListener() {
        try {
            System.out.println("Starting container server.");
            new ContainerServer(exitPassword).start();
        } catch (IOException ex) {
            System.err.println("Failed to start container server.");
        }
    }
}
