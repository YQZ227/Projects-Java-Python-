import java.io.*;
import java.net.*;
import java.net.ServerSocket;
import java.net.Socket;


class Server {
	public static void main(String args[]) throws Exception {
		if (args.length != 1) {
			System.out.println("usage: java Server port");
			System.exit(-1);
		}
		int port = Integer.parseInt(args[0]);
		System.out.println("listening on port " + port);
		while(true) {
			try {
				/*
				  YOUR CODE GOES HERE
				  
				  - add an inner loop to read requests from this connection
				    repeatedly (client may reuse the connection for multiple
				    requests)
				  - for each request, compute an output and send a response
				  - each message has a 4-byte header followed by a payload
				  - the header is the length of the payload
				    (signed, two's complement, big-endian)
				  - the payload is a string (UTF-8)
				  - the inner loop ends when the client closes the connection
				*/


				//- accept a connection from the server socket
				ServerSocket server = new ServerSocket(port);
				System.out.println("Server started");
				System.out.println("Waiting for a client ...");
				Socket socket = server.accept();
				System.out.println("Client accepted");

				server.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
