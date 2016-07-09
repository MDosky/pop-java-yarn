package popjava.yarn.command;

/**
 * To know the current status of the POP Application
 */
public enum POPAppStatus {
    
    KILLED(5),
    FAILED(4),
    FINISHED(3),
    RUNNING(2),
    ACCEPTED(1),
    WAITING(0);
    
    private final int status;

    private POPAppStatus(int status) {
        this.status = status;
    }
    
    public int statusCode() {
        return status;
    }

    /**
     * Give priority to kill if weird status code appears, they shouldn't
     * @return If the application should be killed
     */
    public boolean isKill() {
        switch(status) {
            case 0:
            case 1:
            case 2:
                return false;
            default:
                return true;
        }
    }
}
