package yarn.popjava;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import popjava.baseobject.POPAccessPoint;
import popjava.system.POPSystem;
import popjava.util.Util;
import yarn.popjava.command.AppRoutine;

/**
 * This class is started on every allocated YARN container.
 * Depending on the given attributes it will start a simple DaemonService that
 * will register itself and accept object creation or also start a 
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
            String daemonCmd = System.getProperty("java.home") + "/bin/java -javaagent:popjava.jar -cp popjava.jar:pop-app.jar %s %s %s";
            String cmdImp = String.format(daemonCmd, DaemonService.class.getName(), taskServerAP, "-jobservice=" + jobManagerAP);
            try {
                ProcessBuilder pb = new ProcessBuilder(Util.splitTheCommand(cmdImp));
                pb.redirectOutput(pb.redirectError());
                pb.inheritIO();
                Process popProcess = pb.start();
                // don't wait, we have to execute the main class
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        // start in the thread if there is no main class to start
        else {
            DaemonService.main(taskServerAP, "-jobservice=" + jobManagerAP);
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
        POPSystem.jobService = new POPAccessPoint(jobManagerAP);
        POPSystem.setStarted();
        // start the given main class
        AppRoutine appRoutine = new AppRoutine(taskServerAP);
        try {
            Class clazz = Class.forName(mainClass);
            Method main = clazz.getDeclaredMethod("main", String[].class);
            
            Object[] refArgs = new Object[1];
            String[] argsWjm = new String[args.size() + 1];
            for(int i = 0; i < args.size(); i++)
                argsWjm[i+1] = args.get(i);
            argsWjm[0] = "-jobservice=" + jobManagerAP;
            refArgs[0] = argsWjm;

            main.invoke(null, refArgs);
            
            appRoutine.finish();
        } catch (Exception ex) {
            ex.printStackTrace();
            appRoutine.fail();
        } finally {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ex) {
            }
            // tell everyone to finish their tasks
            appRoutine.waitAndQuit();
        }
    }
}
