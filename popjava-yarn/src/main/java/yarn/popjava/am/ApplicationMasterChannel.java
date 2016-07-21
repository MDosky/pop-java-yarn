package yarn.popjava.am;

import popjava.PopJava;
import popjava.annotation.POPAsyncConc;
import popjava.annotation.POPClass;
import popjava.annotation.POPObjectDescription;
import popjava.baseobject.POPAccessPoint;

/**
 * This is a simple channel to communicate between the AM and something
 * else. Simple because if it is not we will have missing libraries problem.
 * @author Dosky
 */
@POPClass
public class ApplicationMasterChannel {
    
    private ApplicationMasterPOP master;

    // this constructor shouldn't be used
    @POPObjectDescription(url = "localhost")
    public ApplicationMasterChannel() {
        this.master = null;
    }

    @POPObjectDescription(url = "localhost")    
    public ApplicationMasterChannel(String master) {
        this.master = PopJava.newActive(ApplicationMasterPOP.class, new POPAccessPoint(master));
    }
    
    /**
     * Simple rely to master
     * @param memory
     * @param vcores 
     */
    @POPAsyncConc
    public void requestContainer(int memory, int vcores) {
        master.requestContainer(memory, vcores);
    }
    
    /**
     * Simple rely to master
     * @param task
     * @param jobm 
     */
    @POPAsyncConc
    public void setupServers(String task, String jobm) {
        master.setupServers(task, jobm);
    }
}
