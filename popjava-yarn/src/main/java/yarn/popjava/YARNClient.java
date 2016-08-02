package yarn.popjava;

import yarn.popjava.am.ApplicationMaster;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * The class called by the yarn shell command.
 * Here the Application Master container will be asked.
 * From the moment the AM is request the application is Asynchronous and we can
 * only kill by asking the Resource Manager to kill it, be it via a command
 * line interface or a web based one.
 * @author Dosky
 */
public class YARNClient {

    @Parameter(names = "--dir", required = true)
    private String hdfs_dir;
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
        YARNClient c = new YARNClient();
        new JCommander(c, args);
        c.run();
    }

    private Configuration conf;

    public void run() throws Exception {
        // Create yarnClient
        conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        // Create application via yarnClient
        YarnClientApplication app = yarnClient.createApplication();

        String argsString = "";
        for (String s : args) {
            argsString += s + " ";
        }

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer
                = Records.newRecord(ContainerLaunchContext.class);
        amContainer.setCommands(Collections.singletonList(
                          "$JAVA_HOME/bin/java"
                        + " -javaagent:popjava.jar"
                        + " " + ApplicationMaster.class.getName()
                        + " --dir " + hdfs_dir
                        + " --vcores " + vcores
                        + " --memory " + memory
                        + " --containers " + containers
                        + " --main " + main
                        + " " + argsString
                        + " 1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                        + " 2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                        + ";"
                )
        );

        // Setup jar for ApplicationMaster
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        LocalResource appJar = Records.newRecord(LocalResource.class);
        setupAppMasterJar(new Path(hdfs_dir + "/popjava.jar"), appMasterJar);
        setupAppMasterJar(new Path(hdfs_dir + "/pop-app.jar"), appJar);
        Map<String, LocalResource> resources = new HashMap<>();
        resources.put("popjava.jar", appMasterJar);
        resources.put("pop-app.jar", appJar);
        amContainer.setLocalResources(resources);

        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        setupAppMasterEnv(appMasterEnv);
        amContainer.setEnvironment(appMasterEnv);

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(10240);
        capability.setVirtualCores(1);

        // Finally, set-up ApplicationSubmissionContext for the application
        ApplicationSubmissionContext appContext
                = app.getApplicationSubmissionContext();
        appContext.setApplicationName("POP-Java: " + main);
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("default");

        // Submit application
        ApplicationId appId = appContext.getApplicationId();
        System.out.println("Submitting application " + appId);
        yarnClient.submitApplication(appContext);

        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED
                && appState != YarnApplicationState.KILLED
                && appState != YarnApplicationState.FAILED) {
            Thread.sleep(100);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }

        System.out.println(
                "Application " + appId + " finished with"
                + " state " + appState
                + " at " + appReport.getFinishTime());

    }

    private void setupAppMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
        FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }

    private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
                    c.trim(), File.pathSeparator);
        }
        Apps.addToEnvironment(appMasterEnv,
                Environment.CLASSPATH.name(),
                Environment.PWD.$() + File.separator + "*", File.pathSeparator);
    }
}
