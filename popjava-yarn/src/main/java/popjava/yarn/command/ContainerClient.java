package popjava.yarn.command;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

/**
 *
 * @author Dosky
 */
public class ContainerClient extends Thread {

    protected final Socket socket;
    protected final PrintWriter writer;
    protected final Scanner reader;
    
    public ContainerClient(String server) throws IOException {
        this.socket = new Socket(server, ContainerServer.SERVER_PORT);
        this.writer = new PrintWriter(socket.getOutputStream());
        this.reader = new Scanner(socket.getInputStream());
        setName("Master Server tunnel");
        System.out.println("Connected.");
    }

    @Override
    public void run() {
        String line;
        while(reader.hasNext()) {
            line = reader.nextLine();
            System.out.println(line);
            
            if(line.equals("exit")) {
                close();
            }
        }
    }

    private void close() {
        try {
            reader.close();
            writer.close();
            socket.close();
            
            System.exit(0);
        } catch (IOException ex) { }
    }
}
