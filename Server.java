import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.nio.charset.*;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;

public class Server {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("usage: java Server port");
            System.exit(-1);
        }
        ServerSocket server = null;
        int port = Integer.parseInt(args[0]);
        try {
            server = new ServerSocket(port);
        } catch (IOException e) {
            System.out.println("I/O error: " + e);
            System.exit(-1);
        }
        
        while (true) {
            try {
                Socket socket = server.accept(); 
                InputStream inputStream = socket.getInputStream(); 
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                DataInputStream din = new DataInputStream(inputStream);
                while (socket.getInetAddress().isReachable(1000)) {
                    //System.out.println("try successed");
                    // get data from client
                    if (din.available() == 0) {
                        //System.out.println("No data in stream");
                        continue;
                    }
                    int reqstDataLen = din.readInt();
                    //System.out.println("Read int succeed");
                    //System.out.println(reqstDataLen);
                    byte[] bytes = new byte[reqstDataLen];
                    din.readFully(bytes);
                    String input = new String(bytes, StandardCharsets.UTF_8);
                    //System.out.println(input);

                    // process request
                    // 1000 2000
                    String[] lines = input.split(System.lineSeparator());
                    //System.out.println(lines[0]);

                    Map<String, Integer> output = new HashMap<>();
                    for (int i = 0; i < lines.length; ++i) {
                        String line  = lines[i];
                        String[] numbers = line.split(" ");
                        output.put(numbers[0], output.getOrDefault(numbers[0], 0) + 1);
                        output.put(numbers[1], output.getOrDefault(numbers[1], 0) + 1);
                    }
                    //System.out.println("output:" + output);

                    // format the output hashmap into String 
                    String outputString = "";
                    Set<String> keys = output.keySet();
                    for (String key : keys) {
                        outputString += key + " " + output.get(key) + "\n";
                    }

                    //System.out.println(outputString);

                    // // create output file
                    byte[] outputBytes = outputString.getBytes("UTF-8");
                    outputStream.writeInt(outputBytes.length);
                    outputStream.write(outputBytes);
                    outputStream.flush();                
                    //System.out.println("here it is outputstream is saved");
                }
                din.close();
                inputStream.close();
                outputStream.close();
                socket.close();
            } catch (IOException e) {
                System.out.println("I/O error: " + e);
                continue;
            }
            // connection accept and get info from input file
            // - add an inner loop to read requests from this connection
            // repeatedly (client may reuse the connection for multiple
            // requests)
            // - for each request, compute an output and send a response
            // - each message has a 4-byte header followed by a payload
            // - the header is the length of the payload
            // (signed, two's complement, big-endian)
            // - the payload is a string (UTF-8)
            // - the inner loop ends when the client closes the connection
    
        }

    }

}
