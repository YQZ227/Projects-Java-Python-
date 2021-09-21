import java.io.*;
import java.nio.file.*;
import java.nio.charset.*;
import java.net.*;

class Client {
	public static void main(String args[]) throws Exception {
		if (args.length != 4) {
			System.out.println("usage: java Client host port input_file output_file");
			System.exit(-1);
		}
		String host = args[0];
		int port = Integer.parseInt(args[1]);
		String inputFileName = args[2];
		String outputFileName = args[3];

		// step 2: connect to server
		System.out.println("connecting to " + host + ":" + port);
		// 
		Socket sock = new Socket(host, port);
		System.out.println("connected, sending request " + inputFileName);


		// // step 4: receive response from server
		//  DataInputStream din = new DataInputStream(sock.getInputStream());
		//  int respDataLen = din.readInt();
		//  System.out.println("received response header, data payload has length " + respDataLen);

		//  DataOutputStream dout = new DataOutputStream(sock.getOutputStream());
		//  System.out.println("output:" + outputFileName);

		
	 	// step 6: clean up
		sock.close();
		System.out.println("terminated connection to server");

		// note: you should keep the connection open and reuse it
		//        if sending multiple requests back-to-back
	}
		// 
}
 