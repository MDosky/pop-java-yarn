package yarn.popjava;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import popjava.PopJava;
import popjava.annotation.POPAsyncConc;
import popjava.annotation.POPClass;
import popjava.annotation.POPObjectDescription;
import popjava.annotation.POPSyncConc;
import popjava.annotation.POPSyncSeq;

/**
 *
 * @author Dosky
 */
@POPClass
public class ApplicationMasterPOP {

    private Configuration configuration;
    private NMClient nmClient;

    private ApplicationMasterRMCallback rmCallback;
    private AMRMClientAsync<AMRMClient.ContainerRequest> rmClient;

    private Process popProcess;
    private StringBuilder taskServer = new StringBuilder();
    private StringBuilder jobManager = new StringBuilder();

    private int requestedContainers;

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
    private final List<String> args = new ArrayList<>();

    @POPObjectDescription(url = "localhost")
    public ApplicationMasterPOP() {
    }

    @POPObjectDescription(url = "localhost")
    public ApplicationMasterPOP(String... args) {
        new JCommander(this, args);
    }

    @POPSyncSeq
    public void setup() {
        configuration = new YarnConfiguration();

        nmClient = NMClient.createNMClient();
        nmClient.init(configuration);
        nmClient.start();

        rmCallback = new ApplicationMasterRMCallback(nmClient, hdfs_dir, askedContainers, main, args);

        rmClient = AMRMClientAsync.createAMRMClientAsync(100, rmCallback);
        rmClient.init(configuration);
        rmClient.start();

        // start as thread
        PopJava.getThis(this).startCentralServers();
    }

    @POPSyncSeq
    public void runMainLoop() {
        try {
            // Register with ResourceManager
            System.out.println("[AM] registerApplicationMaster 0");
            rmClient.registerApplicationMaster("", 0, "");
            System.out.println("[AM] registerApplicationMaster 1");

            for (int i = 0; i < askedContainers; i++) {
                PopJava.getThis(this).requestContainer(memory, vcores);
            }

            System.out.println("[AM] waiting for containers to finish");
            while (!rmCallback.doneWithContainers()) {
                Thread.sleep(100);
            }

            System.out.println("[AM] unregisterApplicationMaster 0");
            // Un-register with ResourceManager
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
            System.out.println("[AM] unregisterApplicationMaster 1");
        } catch (InterruptedException | YarnException | IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Request a new container
     *
     * @param memory
     * @param vcores
     */
    @POPAsyncConc
    public void requestContainer(int memory, int vcores) {
        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(memory);
        capability.setVirtualCores(vcores);

        // Make container requests to ResourceManager
        AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, null, null, priority);
        System.out.println("[AM] Making reservation request " + requestedContainers++);
        rmClient.addContainerRequest(containerAsk);
    }

    @POPSyncSeq
    public void setParams(String task, String jm) {
        this.taskServer.append(task);
        this.jobManager.append(jm);
    }

    /**
     * Start a clean version of POP-Java which will be used to create processes.
     * Using the same as the one the AM run on pollute the classpath.
     */
    private void startCentralServers() {
        new Thread(() -> {
            List<String> popServer = Lists.newArrayList(
                    System.getProperty("java.home") + "/bin/java",
                    "-javaagent:popjava.jar",
                    "-cp", "popjava.jar:pop-app.jar",
                    ApplicationMasterPOPServer.class.getName()
            );

            System.out.println("--- Creating Server");
            ProcessBuilder pb = new ProcessBuilder(popServer);

            try {
                System.out.println("--- Starting  Server");
                popProcess = pb.start();
                System.out.println("--- Started");
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(popProcess.getInputStream()))) {
                    System.out.println("--- Reading output");
                    String out, taskServer, jobManager;
                    while (!(taskServer = reader.readLine()).startsWith(ApplicationMasterPOPServer.TASK));
                    taskServer = taskServer.substring(ApplicationMasterPOPServer.TASK.length());
                    while (!(jobManager = reader.readLine()).startsWith(ApplicationMasterPOPServer.JOBM));
                    jobManager = jobManager.substring(ApplicationMasterPOPServer.JOBM.length());

                    setParams(taskServer, jobManager);

                    System.out.println("--- Ended " + taskServer + "  " + jobManager);
                    while ((out = reader.readLine()) != null) {
                        System.err.println(out);
                    }
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }).start();
    }
}
