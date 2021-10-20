import java.util.ArrayList;
import java.util.List;
import java.util.*;
import org.mindrot.jbcrypt.BCrypt;
import java.net.InetAddress;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

public class BcryptServiceHandler implements BcryptService.Iface {
	private List<Handler> handlerList = new ArrayList<Handler>();
	private boolean addFEClient = false; // Have we created two FE Client and add them to Client_List?
	private int handlerPtr = 0; // a pointer to last used socket in Handler.
	private int clientNum = 0; // Based on one socket we can create two client, can only be 0 or 1

	public BcryptServiceHandler() {
		addFEClient = true;
	}

	public BcryptServiceHandler(String portFE) {
		
		Handler Fehanlder = new Handler(getHostName(), portFE);
		System.out.println(Fehanlder);
		handlerList.add(Fehanlder);
	}

	//If we are a FEnode, set the portFE we are listening to

	//this service is used for FE to distrube jobs, only FE should be called with this service
	public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
		try {
			// List<String> ret = new ArrayList<>();
			// BcryptService.Client client = getNextClinet();
			// for (int i=1; i<=10;i++){
			// 	ret = client.hashPasswordJob(password, (short)10);
			// }
			// return ret;
			BcryptService.Client client = getClient();
			return client.hashPasswordJob(password, (short)10);
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}

	// this service is to actually doing the hashing job
	public List<String> hashPasswordJob(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException{
		System.out.println("hashing "+ String.valueOf(password.size())+ " passwords");
		List<String> ret = new ArrayList<>();
		for (String pw: password){
			String oneHash = BCrypt.hashpw(pw, BCrypt.gensalt(logRounds));
			ret.add(oneHash);
		}
		return ret;
	}

	public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
	{
		try {
			List<Boolean> ret = new ArrayList<>();
			//System.out.println("check size "+String.valueOf(hash.size()));
			if (password.size() != hash.size()) {
				throw new IllegalArgument("The size of password and size of hash do not match");
			}

			for(int i=0; i< password.size(); i++){
				String onePwd = password.get(i);
				String oneHash = hash.get(i);
				ret.add(BCrypt.checkpw(onePwd, oneHash));
			}

			return ret;
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}

	public void addBE(String ipaddr, String port) throws IllegalArgument, org.apache.thrift.TException {
		try {
			// add the socket to handlerList
			System.out.println("add BE socket at " + ipaddr+":"+port );
			handlerList.add(0, new Handler(ipaddr, port));
			System.out.println(handlerList);
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		} 
	}

	private synchronized BcryptService.Client getClient() throws Exception {
		int hd = getHandlerPtr();
		System.out.println("Handler ptr:" + String.valueOf(hd));
		return createClient(handlerList.get(handlerPtr));
	}
		
	private synchronized int getHandlerPtr() throws Exception {
		int ret = handlerPtr;
		if (clientNum == 0) {
			clientNum = 1;
		} else { //clientNum = 1
			clientNum = 0;
			if (handlerPtr == handlerList.size()-1 ){
				handlerPtr = 0;
			} else {
				handlerPtr += 1;
			}
		}
		return ret;
	}

	private BcryptService.Client createClient(Handler hl) throws Exception {
		TSocket sock = new TSocket(hl.getIpaddr(), hl.getPort());
		TTransport transport = new TFramedTransport(sock);
		TProtocol protocol = new TBinaryProtocol(transport);
		BcryptService.Client client = new BcryptService.Client(protocol);
		transport.open();
		return client;
	}

	static String getHostName()
	{
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			return "localhost";
		}
	}

	public class Handler {
		private String ipaddr;
		private String port;

		public Handler(String ipaddr, String port){
			this.ipaddr = ipaddr;
			this.port = port;
		}

		public String getIpaddr() {
			return ipaddr;
		}

		public int getPort() {
			return Integer.parseInt(port);
		}
	}
}
