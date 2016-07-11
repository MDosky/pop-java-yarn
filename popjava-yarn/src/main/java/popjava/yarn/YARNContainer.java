package popjava.yarn;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import popjava.system.POPSystem;
import popjava.util.Util;
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
            String daemonCmd = System.getProperty("java.home") + "/bin/java -cp popjava.jar:pop-app.jar popjava.yarn.DaemonService %s %s";
            try {
                runCmd(String.format(daemonCmd, taskServerAP, "-jobservice=" + jobManagerAP));
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        } // start in the thread if there is only a single daemon
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
        POPSystem.initialize("-jobservice=" + jobManagerAP);
        // start the given main class
        AppRoutine appRoutine = new AppRoutine(taskServerAP);
        try {
            String mainCmdFormat = System.getProperty("java.home") + "/bin/java -javaagent:popjava.jar -cp popjava.jar:pop-app.jar %s %s %s";
            String mainCmd = String.format(mainCmdFormat, mainClass, "-jobservice=" + jobManagerAP, groupList(args));
            System.out.println(System.currentTimeMillis() + " start main");
            runCmd(mainCmd);
            appRoutine.finish();
            System.out.println(System.currentTimeMillis() + " end main");
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

    /**
     * Take a list of object and turn it into a String to use as parameter
     *
     * @param args [ E1, E2, ... ]
     * @return E1.toString E2.toString ...
     */
    private String groupList(List args) {
        StringBuilder out = new StringBuilder();
        for (Object arg : args) {
            out.append(arg.toString()).append(" ");
        }

        return out.toString();
    }

    /**
     * Run a given command string
     *
     * @param cmd A linux command
     */
    private void runCmd(String cmd) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(Util.splitTheCommand(cmd));
        pb.inheritIO();
        Process popProcess = pb.start();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(popProcess.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }
    }

}
