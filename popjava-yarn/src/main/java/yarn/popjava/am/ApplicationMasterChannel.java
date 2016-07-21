package yarn.popjava.am;

import popjava.PopJava;
import popjava.annotation.POPAsyncConc;
import popjava.annotation.POPClass;
import popjava.annotation.POPObjectDescription;
import popjava.annotation.POPSyncConc;
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
    }
    
    @POPSyncConc
    public void setMaster(POPAccessPoint master) {
        this.master = PopJava.newActive(ApplicationMasterPOP.class, master);
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
