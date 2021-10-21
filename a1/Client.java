import java.util.List;
import java.util.Random;
import java.util.ArrayList;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

public class Client {
	// public static void main(String [] args) {
	// 	if (args.length != 3) {
	// 		System.err.println("Usage: java Client FE_host FE_port password");
	// 		System.exit(-1);
	// 	}

	// 	try {
	// 		TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
	// 		TTransport transport = new TFramedTransport(sock);
	// 		TProtocol protocol = new TBinaryProtocol(transport);
	// 		BcryptService.Client client = new BcryptService.Client(protocol);
	// 		transport.open();

	// 		List<String> password = new ArrayList<>();
	// 		password.add(args[2]);
	// 		List<String> hash = client.hashPassword(password, (short)10);
	// 		System.out.println("Password: " + password.get(0));
	// 		System.out.println("Hash: " + hash.get(0));
	// 		System.out.println("Positive check: " + client.checkPassword(password, hash));
	// 		hash.set(0, "$2a$14$reBHJvwbb0UWqJHLyPTVF.6Ld5sFRirZx/bXMeMmeurJledKYdZmG");
	// 		System.out.println("Negative check: " + client.checkPassword(password, hash));
	// 		try {
	// 			hash.set(0, "too short");
	// 			List<Boolean> rets = client.checkPassword(password, hash);
	// 			System.out.println("Exception check: no exception thrown");
	// 		} catch (Exception e) {
	// 			System.out.println("Exception check: exception thrown");
	// 		}

	// 		transport.close();
	// 	} catch (TException x) {
	// 		x.printStackTrace();
	// 	} 
	// }

	public static void main(String [] args) {
		if (args.length != 5) {
	 		System.err.println("Usage: java Client FE_host FE_port #Threads #Requests #pwd/Req");
			System.exit(-1);
		}

		String ipAddr = args[0];
		int portFE = Integer.parseInt(args[1]);
		int threadNum = Integer.parseInt(args[2]);
		int reqNum = Integer.parseInt(args[3]);
		int pwdPerReq = Integer.parseInt(args[4]);

		for(int i=1; i<=threadNum; i++) {
			Thread t1 = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						generate_thread(ipAddr, portFE,reqNum,pwdPerReq);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			System.out.println("Thread "+String.valueOf(i)+  " start");
			t1.start();
		}
	}

	private static void generate_thread(String ipaddr, int port, 
	int requestNum, int pwdPerReq) {
		try {
		TSocket sock = new TSocket(ipaddr, port);
	 	TTransport transport = new TFramedTransport(sock);
		TProtocol protocol = new TBinaryProtocol(transport);
 		BcryptService.Client client = new BcryptService.Client(protocol);
		transport.open();

		for (int j=1; j<=requestNum; j++ ){
			//generate passward list
			List<String> password = new ArrayList<>();
			for (int i=1; i<=pwdPerReq; i++){
				password.add(generate_password());
			}
			
			double s = 10;
			double n = 4096/(Math.pow(2,s));
			long hashStartTime = System.currentTimeMillis();
			List<String> hash = client.hashPassword(password, (short)s);
			long hashEndTime = System.currentTimeMillis();
			System.out.println("Hashing password finished ");
			System.out.println("Throughput for hasing logRounds = " + s + ": " + n * 1000f/(hashEndTime-hashStartTime));
			System.out.println("Latency for hashinglogRounds = " + s + ": " + (hashEndTime-hashStartTime)/n);
			System.out.println("Final hashing result =" + s + ": " + 6/(hashEndTime-hashStartTime)/(n  * 1000f));


			System.out.println(client.checkPassword(password, hash));
			long checkEndTime = System.currentTimeMillis();
			System.out.println("Checking password finished ");
			System.out.println("Throughput for checking logRounds = " + s + ": " + 32 * 1000f/(checkEndTime-hashEndTime));
			System.out.println("Latency for checking logRounds = " + s + ": " + (checkEndTime-hashEndTime)/32);
			System.out.println("Final checking result =" + s + ": " + 6/(checkEndTime-hashEndTime)/(32 * 1000f));
		}
		} catch (TException x) {
			x.printStackTrace();
		} 
	}

	private static String generate_password() {
		int length = 1024;
		String str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    	Random random=new Random();
     	StringBuffer sb=new StringBuffer();
		for(int i=0;i<length;i++){
			int number=random.nextInt(62);
			sb.append(str.charAt(number));
		}
     return sb.toString();
	}
}
