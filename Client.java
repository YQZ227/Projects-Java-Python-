import java.io.*;
import java.nio.file.*;
import java.nio.charset.*;
import java.net.*;

class Client {
    public static void main(String args[]) throws Exception {
        if (args.length != 6) {
            System.out.println("usage: java Client host port input_file output_file");
            System.exit(-1);
        }
        String host = args[0];
        //host = "127.0.0.1";
        int port = Integer.parseInt(args[1]);
        String inputFileName = args[2];
        String outputFileName = args[3];

        String inputFileName2 = args[4];
        String outputFileName2 = args[5];

        // step 1: read graph into byte array
        String input = new String(Files.readAllBytes(Paths.get(inputFileName)), StandardCharsets.UTF_8);
        System.out.println("read input from " + inputFileName);

        String input2 = new String(Files.readAllBytes(Paths.get(inputFileName2)), StandardCharsets.UTF_8);
        System.out.println("read input from " + inputFileName2);

        // step 2: connect to server
        System.out.println("connecting to " + host + ":" + port);
        Socket sock = new Socket(host, port);
        System.out.println("connected, sending request " + inputFileName);

        // step 3: send request to server

        DataOutputStream dout = new DataOutputStream(sock.getOutputStream());
        byte[] bytes = input.getBytes("UTF-8");
        long startTime = System.currentTimeMillis();
        dout.writeInt(bytes.length);
        dout.write(bytes);
        dout.flush();
        System.out.println("sent request header and " + bytes.length + " bytes of payload data to server");

        // step 4: receive response from server
        DataInputStream din = new DataInputStream(sock.getInputStream());
        int respDataLen = din.readInt();

        System.out.println("received response header, data payload has length " + respDataLen);

        bytes = new byte[respDataLen];
        din.readFully(bytes);
        long endTime = System.currentTimeMillis();
        System.out.println(
                "received " + bytes.length + " bytes of payload data from server in " + (endTime - startTime) + "ms");
        String output = new String(bytes, StandardCharsets.UTF_8);

        // step 5: save to file
        Files.write(Paths.get(outputFileName), output.getBytes("UTF-8"));
        System.out.println("wrote output to " + outputFileName);


        // step 3: send request to server
        DataOutputStream dout2 = new DataOutputStream(sock.getOutputStream());
        byte[] bytes2 = input2.getBytes("UTF-8");
        long startTime2 = System.currentTimeMillis();
        dout2.writeInt(bytes2.length);
        dout2.write(bytes2);
        dout2.flush();
        System.out.println("sent request header and " + bytes2.length + " bytes of payload data to server");

        // step 4: receive response from server
        DataInputStream din2 = new DataInputStream(sock.getInputStream());
        int respDataLen2 = din2.readInt();

        System.out.println("received response header, data payload has length " + respDataLen2);

        bytes2 = new byte[respDataLen2];
        din2.readFully(bytes2);
        long endTime2 = System.currentTimeMillis();
        System.out.println(
                "received " + bytes2.length + " bytes of payload data from server in " + (endTime2 - startTime2) + "ms");
        String output2 = new String(bytes2, StandardCharsets.UTF_8);

        // step 5: save to file
        Files.write(Paths.get(outputFileName2), output2.getBytes("UTF-8"));
        System.out.println("wrote output to " + outputFileName2);

        // step 6: clean up
        sock.close();
        System.out.println("terminated connection to server");

        // note: you should keep the connection open and reuse it
        // if sending multiple requests back-to-back
    }
}