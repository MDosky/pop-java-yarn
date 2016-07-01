package popjava.yarn.command;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 *
 * @author Dosky
 */
public class ContainerServer extends Thread {

    public static final int SERVER_PORT = 7689;

    private final ServerSocket server;
    private final String secret;
    
    private final List<ServerThread> threads;

    public ContainerServer(String secret) throws IOException {
        this.server = new ServerSocket(SERVER_PORT);
        this.secret = secret;
        this.threads = new ArrayList<>();
    }

    @Override
    public void run() {
        Executor executor = Executors.newCachedThreadPool();
        System.out.println("Starting container controller");

        try {
            while (!Thread.currentThread().isInterrupted()) {
                Socket socket = server.accept();
                System.out.println("Accepted connection from " + socket.getInetAddress().getHostAddress());
                ServerThread thread = new ServerThread(socket);
                executor.execute(thread);
                threads.add(thread);
            }
        } catch (IOException e) {
        }
    }

    public void closeAll() {
        for (ServerThread thread : threads) {
            if(thread.socket.isClosed())
                continue;
            thread.close();
        }
    }

    public class ServerThread implements Runnable {

        private final Socket socket;
        private final PrintWriter writer;
        private final Scanner reader;

        public ServerThread(Socket socket) throws IOException {
            this.socket = socket;
            this.writer = new PrintWriter(socket.getOutputStream());
            this.reader = new Scanner(socket.getInputStream());
        }

        @Override
        public void run() {
            String line;
            while (reader.hasNext()) {
                line = reader.nextLine();
                System.out.format("%s: %s\n", socket.getInetAddress().getHostAddress(), line);
                
                // command and reaction
                if(line.startsWith("exit")) {
                    if(line.substring(4).equals(secret)) {
                        closeAll();
                    } else {
                        System.out.format("%s: %s\n", socket.getInetAddress().getHostAddress(), "Wrong secret.");
                        writer.println("400 Wrong secret.");
                    }
                }
            }
        }

        public void close() {
            try {
                writer.flush();
                writer.println("exit");
                
                writer.close();
                reader.close();
                socket.close();
            } catch (IOException ex) {
            }
        }
    }
}
