package popjava.yarn;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
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
    @Parameter(names = "-daemon")
    private List<String> daemons;
    @Parameter(names = "-myDaemon", required = true)
    private String myDaemon;
    @Parameter
    private List<String> args;
    
    public static void main(String[] args) {
        YARNContainer container = new YARNContainer();
        new JCommander(container, args);
        container.start();
    }

    private YARNContainer() {
        daemons = new ArrayList<>();
        args = new ArrayList<>();
    }
    
    /**
     * Actual start of the program
     */
    private void start() {
        // start daemon on every container
        startDaemon();
        
        // start JM and the main if is the main container
        if(main)
            startMainContainer();
    }

    /**
     * Start the POP-Java Daemon process
     */
    private void startDaemon() {
        DaemonInfo di = new DaemonInfo(myDaemon);
        String daemonCmd = env("JAVA_HOME") + "/bin/java -cp popjava.jar:yarn-app.jar popjava.yarn.DaemonService %s";
        runCmd(String.format(daemonCmd, di.toString()));
    }

    /**
     * Start the JobManager and then the POP Main class
     */
    private void startMainContainer() {
        String popjava = env("JAVA_HOME") + "/bin/java -javaagent:popjava.jar -cp popjava.jar:yarn-app.jar %s %s";
        
        // start JM
        runCmd(String.format(popjava, "popjava.jobmanager.POPJavaJobManager", groupList(daemons)));
        
        // give it time to start
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ex) { }
        
        // start main class
        runCmd(String.format(popjava, mainClass, groupList(args)));
    }
    
    /**
     * Run a given command string
     * @param cmd A linux command
     */
    private void runCmd(String cmd) {
        List<String> cmdList = Arrays.asList(cmd.split(" "));
        SystemUtil.runCmd(cmdList);
    }
    
    /**
     * Take a list of object and turn it into a String to use as parameter
     * @param args [ E1, E2, ... ]
     * @return E1.toString E2.toString ...
     */
    private String groupList(List args) {
        StringBuilder out = new StringBuilder();
        for(Object arg : args)
            out.append(arg.toString()).append(" ");
        
        return out.toString();
    }
    
    private String env(String var) {
        return System.getProperty(var);
    }
}
