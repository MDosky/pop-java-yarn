package popjava.yarn;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import popjava.service.DaemonInfo;
import popjava.util.SystemUtil;

/**
 *
 * @author Dosky
 */
public class YARNContainer {

    @Parameter(names = "-main")
    private boolean main;
    @Parameter(names = "-mainClass")
    private String mainClass;
    @Parameter(names = "-taskServer", required = true)
    private String taskServerAP;
    @Parameter
    private List<String> args;

    public static void main(String[] args) {
        YARNContainer container = new YARNContainer();
        new JCommander(container, args);
        container.start();
    }

    private YARNContainer() {
        args = new ArrayList<>();
    }

    /**
     * Actual start of the program
     */
    private void start() {
        // start daemon on every container
        startDaemon();

        // start JM and the main if is the main container
        if (main) {
            startMainContainer();
        }
    }

    /**
     * Start the POP-Java Daemon process
     */
    private void startDaemon() {
        // start in parallel if it's the main class
        if (main) {
            String daemonCmd = javaHome() + "/bin/java -cp popjava.jar:pop-app.jar popjava.yarn.DaemonService %s";
            runCmd(String.format(daemonCmd, taskServerAP));
        } 
        // start in the thread if there is only a single daemon
        else {
            DaemonService.main(taskServerAP);
        }
    }

    /**
     * Start the JobManager and then the POP Main class
     */
    private void startMainContainer() {
//        String popjava = javaHome() + "/bin/java -javaagent:popjava.jar -cp popjava.jar:pop-app.jar %s %s";

        // start JM
//        runCmd(String.format(popjava, "popjava.jobmanager.POPJavaJobManager"));

        // give it time to start
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ex) {
        }

        // start the given main class
        try {
            Class clazz = Class.forName(mainClass.substring(mainClass.lastIndexOf(".")));
            Method method = clazz.getMethod("main", String[].class);
            method.invoke(null, args.toArray(new String[0]));
        } catch (ClassNotFoundException ex) {
            System.out.println("Main class not found.");
            ex.printStackTrace();
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            System.out.println("main method not found in Main class.");
            ex.printStackTrace();
        }
    }

    /**
     * Run a given command string
     *
     * @param cmd A linux command
     */
    private void runCmd(String cmd) {
        List<String> cmdList = Arrays.asList(cmd.split(" "));
        SystemUtil.runCmd(cmdList);
    }

    /**
     * Java home location
     *
     * @return
     */
    private String javaHome() {
        return System.getenv("JAVA_HOME");
    }
}
