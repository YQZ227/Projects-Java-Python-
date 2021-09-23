import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.nio.charset.*;
import java.util.Map;
import java.util.HashMap;

public class Ser {
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

            try (Socket socket = server.accept(); 
            InputStream inputStream = socket.getInputStream(); 
            DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
                System.out.println("try successed");
                // get data from client
                DataInputStream din = new DataInputStream(inputStream);
                int reqstDataLen = din.readInt();
                System.out.println(reqstDataLen);
                byte[] bytes = new byte[reqstDataLen];
                din.readFully(bytes);
                String input = new String(bytes, StandardCharsets.UTF_8);
                //System.out.println(input);

                // process request
                // 1000 2000
                String[] lines = input.split(System.lineSeparator());
                System.out.println(lines[0]);

                Map<String, Integer> output = new HashMap<>();
                for (int i = 0; i < lines.length; ++i) {
                    String line  = lines[i];
                    String[] numbers = line.split(" ");
                    output.put(numbers[0], output.getOrDefault(numbers[0], 0) + 1);
                    output.put(numbers[1], output.getOrDefault(numbers[1], 0) + 1);
                }
                System.out.println("output:" + output);

                // format the output hashmap into String 
                
                String outputString = output.toString().replace(",","\n" ).replace("{"," " ).replace("}"," " ).replace("="," " );;
                System.out.println(outputString);

                // create output file
                FileOutputStream fileout = null;
                DataOutputStream  dataOut = null;
                fileout = new FileOutputStream("Output.txt");
                byte[] outputBytes = outputString.getBytes("UTF-8");
                dataOut = new DataOutputStream(fileout);
                dataOut.writeInt(outputBytes.length);
                dataOut.write(outputBytes);
                dataOut.flush();
                dataOut.close();
                fileout.close();
                
                System.out.println("here it is outputstream is saved" );

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
           
            // try {
            //      input = socket.getInputStream();

            // } catch (IOException e) {
            //     System.out.println("Cannot get input stream");
            // }
            // try {
            //      output = new FileOutputStream("test.out");
            // } catch (FileNotFoundException e) {
            //     System.out.println("File not found");
            // }
            // byte[] bytes = new byte[16 * 1024];
            // int count;
            // while ((count = input.read(bytes)) > 0) {
            //     output.write(bytes, 0, count);
            // }

            // output.close();
            // input.close();
            // socket.close();
        }

    }

}
