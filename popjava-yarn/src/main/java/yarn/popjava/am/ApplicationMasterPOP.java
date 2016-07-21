package yarn.popjava.am;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
import popjava.base.POPObject;

/**
 *
 * @author Dosky
 */
@POPClass
public class ApplicationMasterPOP extends POPObject {

    private Configuration configuration;
    private NMClient nmClient;

    private ApplicationMasterRMCallback rmCallback;
    private AMRMClientAsync<AMRMClient.ContainerRequest> rmClient;

    private Process popProcess;
    private String taskServer;
    private String jobManager;

    private final AtomicInteger requestedContainers = new AtomicInteger();
    private ApplicationMasterChannel channel;

    private boolean ready = false;

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

    @POPAsyncConc
    public void setup() {
        configuration = new YarnConfiguration();

        nmClient = NMClient.createNMClient();
        nmClient.init(configuration);
        nmClient.start();

        rmCallback = new ApplicationMasterRMCallback(nmClient, hdfs_dir, askedContainers, main, args);

        rmClient = AMRMClientAsync.createAMRMClientAsync(100, rmCallback);
        rmClient.init(configuration);
        rmClient.start();
        
        // setup channel
        channel = PopJava.newActive(ApplicationMasterChannel.class, getAccessPoint().toString());

        // start as thread
        PopJava.getThis(this).startCentralServers();
    }

    @POPSyncConc
    public boolean isReady() {
        return ready;
    }

    @POPSyncSeq
    public void runMainLoop() {
        if (!ready) {
            return;
        }

        try {
            // set servers
            rmCallback.setServer(taskServer, jobManager);

            // Register with ResourceManager
            System.out.println("[AM] registerApplicationMaster 0%");
            rmClient.registerApplicationMaster("", 0, "");
            System.out.println("[AM] registerApplicationMaster 100%");

            for (int i = 0; i < askedContainers; i++) {
                PopJava.getThis(this).requestContainer(memory, vcores);
            }

            System.out.println("[AM] waiting for containers to finish");
            while (!rmCallback.doneWithContainers()) {
                Thread.sleep(100);
            }

            System.out.println("[AM] unregisterApplicationMaster 0%");
            // Un-register with ResourceManager
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
            System.out.println("[AM] unregisterApplicationMaster 100%");
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
        // kind of sanitize
        memory = memory < 256 ? 256 : memory;
        vcores = vcores < 1 ? 1 : vcores;
        
        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(memory);
        capability.setVirtualCores(vcores);

        // Make container requests to ResourceManager
        AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, null, null, priority);
        System.out.println("[AM] Making reservation request " + requestedContainers.getAndIncrement());
        rmClient.addContainerRequest(containerAsk);
    }
    
    /**
     * Setup servers AP and be ready to deploy
     * @param task
     * @param jobm 
     */
    @POPAsyncConc
    public void setupServers(String task, String jobm) {
        taskServer = task;
        jobManager = jobm;
        ready = true;
    }

    /**
     * Start a clean version of POP-Java which will be used to create processes.
     * Using the same as the one the AM run on pollute the classpath.
     */
    @POPAsyncConc
    public void startCentralServers() {
        try {
            System.out.println("[AM] Creating servers command");
            List<String> popServer = Lists.newArrayList(
                    System.getProperty("java.home") + "/bin/java",
                    "-javaagent:popjava.jar",
                    "-cp", "popjava.jar:pop-app.jar",
                    ApplicationMasterPOPServer.class.getName(),
                    PopJava.getAccessPoint(channel).toString()
            );
            System.out.println("[AM] Command is " + Arrays.toString(popServer.toArray()));

            System.out.println("[AM] Starting process");

            ProcessBuilder pb = new ProcessBuilder(popServer);

            System.out.println("[AM] Started process");
            popProcess = pb.start();

            this.printStream(popProcess.getInputStream(), true);
            this.printStream(popProcess.getErrorStream(), false);

            System.out.println("[AM] Getting servers");

            popProcess.waitFor();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Print a stream and handle it if required.
     * Not a POP method because InputStream is not serializable.
     * @param is
     * @param parseServer
     * @param err 
     */
    private void printStream(InputStream is, boolean parseServer) {
        new Thread(() -> {
            final PrintStream pw = !parseServer ? System.err : System.out;

            boolean t, j = t = false;
            String line;
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            try {
                while ((line = br.readLine()) != null) {
                    // get servers
//                    if (parseServer) {
//                        // server start setup
//                        if (!t || !j) {
//                            // get and set task server for containers
//                            if (line.startsWith(ApplicationMasterPOPServer.TASK)) {
//                                taskServer = line.substring(ApplicationMasterPOPServer.TASK.length());
//                                t = true;
//                            }
//                            // get and set jobmanager for containers
//                            else if (line.startsWith(ApplicationMasterPOPServer.JOBM)) {
//                                jobManager = line.substring(ApplicationMasterPOPServer.JOBM.length());
//                                j = true;
//                            }
//                            // both are set, main can now run
//                            if (t && j)
//                                ready = true;
//                        }
//                        
//                        // handle commands from JM written in sysout
//                        if (line.startsWith(JobManagerAllocator.MSG_ALLOC)) {
//                            // remove identifier
//                            line = line.substring(JobManagerAllocator.MSG_ALLOC.length()).trim();
//                            // get params
//                            String[] values = line.split("\\s+");
//                            int memory = (int) Float.parseFloat(values[0]);
//                            int power  = (int) Float.parseFloat(values[1]);
//                            
//                            // ask for new alloc
//                            PopJava.getThis(this).requestContainer(memory, vcores);
//                        }
//                    }
                    
                    pw.println(line);
                }
            } catch (IOException ex) {
            }
        }).start();
    }
}
