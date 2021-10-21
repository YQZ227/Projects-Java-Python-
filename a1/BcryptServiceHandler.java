import java.util.ArrayList;
import java.util.List;
import java.util.*;
import org.mindrot.jbcrypt.BCrypt;
import java.net.InetAddress;
import java.util.concurrent.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

public class BcryptServiceHandler implements BcryptService.Iface {
	private List<Handler> handlerList = new ArrayList<Handler>();
	private int handlerPtr = 0; // a pointer to last used socket in Handler.

	public BcryptServiceHandler() {
	}

	public BcryptServiceHandler(String portFE) {
		Handler Fehanlder = new Handler(getHostName(), portFE);
		handlerList.add(Fehanlder);
		handlerList.add(Fehanlder);
	}

	//this service is used for FE to distrube jobs, only FE should be called with this service
	public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
		try {
			if (password.size()> 1) {
				int pwdP = (int)Math.ceil((double) password.size()/(handlerList.size()));
				List<FutureTask<List<String>>> FtList = new ArrayList<>();
				for(int i=0; i<password.size(); i+= pwdP ) {
					BcryptService.Client client = getClient();
					int end = i+pwdP;
					if (end > password.size() - 1) {
						end = password.size();
					}
					List<String> sub = password.subList(i, end);
					FutureTask<List<String>> Ft = new FutureTask<>(new HashCallable(sub, client));
					Thread thread = new Thread(Ft);
					FtList.add(Ft);
					thread.start();
				}
				List<String> result = new ArrayList<>();
				for (FutureTask<List<String>> Ft: FtList) {
					result.addAll(Ft.get());
				}
				return result;
			} else {
				BcryptService.Client client = getClient();
				return client.hashPasswordJob(password, (short)10);
			}
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

	public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
		try {
			if (password.size() != hash.size()) {
				throw new IllegalArgument("The size of password and size of hash do not match");
			}
	
			if (password.size()> 1) {
				int pwdP = (int)Math.ceil((double) password.size()/(handlerList.size()));
				List<FutureTask<List<Boolean>>> FtList = new ArrayList<>();
				for(int i=0; i<password.size(); i+= pwdP ) {
					BcryptService.Client client = getClient();
					int end = i+pwdP;
					if (end > password.size() - 1) {
						end = password.size();
					}
					List<String> pwdsub = password.subList(i, end);
					List<String> hashsub = hash.subList(i, end);
					FutureTask<List<Boolean>> Ft = new FutureTask<>(new CheckCallable(pwdsub, hashsub, client));
					Thread thread = new Thread(Ft);
					FtList.add(Ft);
					thread.start();
				}
				List<Boolean> result = new ArrayList<>();
				for (FutureTask<List<Boolean>> Ft: FtList) {
					result.addAll(Ft.get());
				}
				return result;
			} else {
				BcryptService.Client client = getClient();
				return client.checkPasswordJob(password, hash);
			}
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}

	public List<Boolean> checkPasswordJob(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
	{
		try {
			System.out.println("Checking "+ String.valueOf(password.size())+ " passwords");
			List<Boolean> ret = new ArrayList<>();
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
			handlerList.add(0, new Handler(ipaddr, port));
			handlerList.add(0, new Handler(ipaddr, port));
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		} 
	}

	private BcryptService.Client getClient() throws Exception {
		int hd = getHandlerPtr();
		return createClient(handlerList.get(hd));
	}
		
	private synchronized int getHandlerPtr() throws Exception {
		int ret = handlerPtr;
		if (handlerPtr == handlerList.size() - 1){
			handlerPtr = 0;
		} else {
			handlerPtr += 1;
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

	public class HashCallable implements Callable<List<String>> {
		public List<String> password;
		public BcryptService.Client client;
		public HashCallable(List<String> password, BcryptService.Client client) {
			this.password = password;
			this.client = client;
		}

		@Override 
		public List<String> call() throws Exception  {
			List<String> ret = client.hashPasswordJob(password, (short)10);
			return ret;
		} 
	}

	public class CheckCallable implements Callable<List<Boolean>> {
		public List<String> password;
		public List<String> hash;
		public BcryptService.Client client;
		public CheckCallable(List<String> password, List<String> hash, BcryptService.Client client) {
			this.password = password;
			this.client = client;
			this.hash = hash;
		}

		@Override 
		public List<Boolean> call() throws Exception  {
			List<Boolean> ret = client.checkPasswordJob(password, hash);
			return ret;
		} 
	}
}
