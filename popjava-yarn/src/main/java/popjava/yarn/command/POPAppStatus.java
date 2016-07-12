package popjava.yarn.command;

/**
 * To know the current status of the POP Application
 */
public enum POPAppStatus {
    
    KILLED,
    FAILED,
    FINISHED,
    RUNNING,
    ACCEPTED,
    WAITING;
    
    /**
     * Give priority to kill if weird status code appears, they shouldn't
     * @return If the application should be killed
     */
    public boolean isKill() {
        switch(this) {
            case WAITING:
            case ACCEPTED:
            case RUNNING:
                return false;
            default:
                return true;
        }
    }
}
