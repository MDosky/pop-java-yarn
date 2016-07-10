package popjava.yarn;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import popjava.PopJava;
import popjava.jobmanager.POPJavaJobManager;
import popjava.system.POPSystem;
import popjava.yarn.command.POPAppStatus;
import popjava.yarn.command.TaskServer;

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

//    private final Map<Long, DaemonInfo> daemonInfo = new HashMap<>();
//    private final Random rnd = new SecureRandom();
//    private final String exitPassword;
//    private int lastPort = POPJavaDeamon.POP_JAVA_DEAMON_PORT;
    
    private TaskServer taskServer;
    private POPJavaJobManager jobManager;

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
                taskServer.setStatus(POPAppStatus.RUNNING);
            }

            System.out.println("[AM] Starting client");
            // Launch container by create ContainerLaunchContext
            ContainerLaunchContext ctx
                    = Records.newRecord(ContainerLaunchContext.class);
            List script = Lists.newArrayList(
                "$JAVA_HOME/bin/java"
                + " -javaagent:popjava.jar"
                + " popjava.yarn.YARNContainer"
                + " -taskserver " + taskServer.getAccessPoint().toString()
                + " -jobmanager " + jobManager.getAccessPoint().toString()
                + " " + mainStarter
                + " 1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                + " 2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                + ";"
            );
            System.out.println("[AM] Executing: " + Arrays.toString(script.toArray(new String[0])));
            ctx.setCommands(script);

            // resources
            LocalResource popJar = Records.newRecord(LocalResource.class);
            LocalResource appJar = Records.newRecord(LocalResource.class);
            try {
                setupClientJar(new Path(hdfs_dir + "/pop-app.jar"), popJar);
                setupClientJar(new Path(hdfs_dir + "/yarn-app.jar"), popJar);
            } catch (IOException ex) {}
            Map<String, LocalResource> resources = new HashMap<>();
            resources.put("pop-app.jar", popJar);
            resources.put("yarn-app.jar", appJar);
            ctx.setLocalResources(resources);
            
            // env variables
            Map<String, String> appEnv = new HashMap<String, String>();
            for (String c : configuration.getStrings(
                    YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
                Apps.addToEnvironment(appEnv, ApplicationConstants.Environment.CLASSPATH.name(),
                        c.trim(), ":");
            }
            Apps.addToEnvironment(appEnv,
                    ApplicationConstants.Environment.CLASSPATH.name(),
                    ApplicationConstants.Environment.PWD.$() + File.separator + "*", ":");
            
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

        // server status, accepted
        taskServer.setStatus(POPAppStatus.ACCEPTED);
        
        System.out.println("[AM] waiting for containers to finish");
        while (!doneWithContainers()) {
            Thread.sleep(100);
        }

        System.out.println("[AM] unregisterApplicationMaster 0");
        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
        System.out.println("[AM] unregisterApplicationMaster 1");
        
        // quit pop java
        POPSystem.end();
    }

    private void setupClientJar(org.apache.hadoop.fs.Path jarPath, LocalResource clientJar) throws IOException {
        FileStatus jarStat = FileSystem.get(configuration).getFileStatus(jarPath);
        clientJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        clientJar.setSize(jarStat.getLen());
        clientJar.setTimestamp(jarStat.getModificationTime());
        clientJar.setType(LocalResourceType.FILE);
        clientJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }

//    private String generatePassword() {
//        return new BigInteger(256, rnd).toString(Character.MAX_RADIX);
//    }

    private void startCentralServers() {
        POPSystem.initialize();
        taskServer = PopJava.newActive(TaskServer.class);
        jobManager = PopJava.newActive(POPJavaJobManager.class);
        
        taskServer.setJobManager(jobManager.getAccessPoint());
        // server status, waiting
        taskServer.setStatus(POPAppStatus.WAITING);
    }
}
