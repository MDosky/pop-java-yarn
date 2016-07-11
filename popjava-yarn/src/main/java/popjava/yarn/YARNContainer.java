package popjava.yarn;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import popjava.system.POPSystem;
import popjava.util.SystemUtil;
import popjava.yarn.command.AppRoutine;

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
    @Parameter(names = "-jobmanager", required = true)
    private String jobManagerAP;
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
            String daemonCmd = System.getenv("JAVA_HOME") + "/bin/java -cp popjava.jar:pop-app.jar popjava.yarn.DaemonService %s";
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
        // give it time to start
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ex) {
        }

        // Init POP-Java
        POPSystem.initialize("-jobservice=" + jobManagerAP);
        // start the given main class
        AppRoutine appRoutine = new AppRoutine(taskServerAP);
        try {
            // http://stackoverflow.com/questions/15582476/how-to-call-main-method-of-a-class-using-reflection-in-java
            final Object[] refArgs = new Object[1];
            refArgs[0] = args.toArray(new String[0]);
            
            // call main with reflection in this thread
            final Class clazz = Class.forName(mainClass);
            final Method method = clazz.getDeclaredMethod("main", String[].class);
            method.invoke(null, refArgs);
        } catch (ClassNotFoundException ex) {
            System.out.println("Main class not found.");
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            System.out.println("main method not found in Main class.");
        } finally {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ex) { }
            appRoutine = new AppRoutine(taskServerAP);
            appRoutine.finish();
            // tell everyone to finish their tasks
            appRoutine.waitAndQuit();
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

}