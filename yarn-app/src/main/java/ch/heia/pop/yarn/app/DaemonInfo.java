package ch.heia.pop.yarn.app;

/**
 * This class contain the credential of a specific POPJavaDeamon
 * its password, the port to connect to it and its name just in case.
 * These informations are going to be sent to the various YARN container and
 * processed via command line, so not much security is in place atm.
 * @author Dosky
 */
public class DaemonInfo {
    protected String password;
    protected int port;
    protected long id;

    public DaemonInfo(String password, int port, long id) {
        this.password = password;
        this.port = port;
        this.id = id;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public long getId() {
        return id;
    }

    public void setId(long name) {
        this.id = name;
    }
}
