package popjava.yarn.command;

import java.io.IOException;

/**
 *
 * @author Dosky
 */
public class ContainerExit extends ContainerClient {
    
    public static void main(String[] args) throws IOException {
        ContainerExit containerExit = new ContainerExit(args[0], args[1]);
        containerExit.start();
        containerExit.forceExit();
    }

    private final String secret;
    
    public ContainerExit(String server, String secret) throws IOException {
        super(server);
        this.secret = secret;
    }

    private void forceExit() {
        writer.format("exit %s\n", secret);
    }
}
