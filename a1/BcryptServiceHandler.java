import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {

	public boolean has_BEnodes;
	private static Instant lastbatch;

	public BcryptServiceHandler(boolean hasNode) {
		has_BEnodes = hasNode;
	}

	// pre check batch of passwords, find qualified passwords
	public List<String> precheckPassword(List<String> password, short logRounds) throws IllegalArgument, TException {
		if (password.size() == 0) {
			throw new IllegalArgument("The password is empty");
		}
		if (logRounds < 4 || logRounds > 16) {
			throw new IllegalArgument("logRounds should between 4 and 16");
		}
	}

	// hashing and offloading passwords
	public List<String> hashAtBE(List<String> password, short logRounds) throws IllegalArgument, TException {
		if (has_BEnodes) {
			hashPassword(password, logRounds);
		} else {
			offloadHashPassword(password, logRounds);
		}
	}

	// pre check hashed passwords
	public List<String> precheckHash(List<String> password, List<String> hashed) throws IllegalArgument, TException {
		if (password.isEmpty()) {
			throw new IllegalArgument("Password batch is empty");
		}
		if (hashed.isEmpty()) {
			throw new IllegalArgument("Hash list is empty");
		}
		if (password.size()!= hashed.size()) {
			throw new IllegalArgument("The password bathch and hash lists not mathch");
		}
	
	}

	// check hashed and offload at BE
	public List<String> checkAtBE(List<String> password, List<String> hashed) throws IllegalArgument, TException {
		if (has_BEnodes) {
			checkHash(password, logRounds);
		} else {
			offloadCheck(password, logRounds);
		}
	}

	private List<String> hashPassword(List<String> password, short logRounds)
			throws IllegalArgument, org.apache.thrift.TException {
		// BE avaliable && hash passwords
		lastbatch = Instant.now();
		String[] result = new String[passwords.size()];
		try {
			int numberOfThreads = Math.min(password.size(), 4);
			int numberPerThreads = password.size() / numberOfThreads;
			CountDownLatch latch = new CountDownLatch(numberPerThreads);

			if (password.size() > 1) { // password larger than 1
				for (int i = 0; i < password.size(); ++i) {
					int firstHash = i * numberPerThreads;
					int lastHash;
					if (lastHash == numberOfThreads - 1) {
						lashHash = password.size();
					} else {
						lashHash = (i + 1) * numberPerThreads;
					}
					for (int j = firstHash; j < lastHash; ++j) {
						boolean hashed;
						try {
							hashed = BCrypt.checkpw(password.get(j), BCrypt.gensalt(logRounds));
						} catch (Exception e) {
							hashed = false;
						}
						result[j] = hashed;
					}
				}
				executorService.shutdown();
				executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
				System.out.println("finished hashing " + Arrays.asList(result));
				return Arrays.asList(result);
			} else { // only have one password
				List<String> onePwd = new ArrayList<>(password.size());
				for (String onePass : password) {
					onePwd.add(BCrypt.hashpw(onePass, BCrypt.gensalt(logRounds)));
				}
				System.out.println("finished hashing");
				return onePwd;
			}
		} catch (Exception e) {
			System.out.println("Hashing password at BE nodes failed! " + e.getMessage());
			throw new IllegalArgument(e.getMessage());
		}
	}

	private List<String> offloadHashPassword(List<String> password, short logRounds)throws IllegalArgument, TException {
		// pre check password
		precheckPassword(password, logRounds);

		// find a Node
		NodeManager.WorkStatus BEworker = NodeManager.findWorker();
		System.out.println("Find a backend Thread: " + Thread.currentThread().getName() + "Worker Found: " + BEworker);

		while (BEworker != null) {
			try {
				// Connect to Node
				transport = new TFramedTransport(new TSocket(host, Integer.parseInt(port)));
				client = new BcryptService.Client(new TBinaryProtocol(transport));
				TTransport transportToBE = BEworker.getTransport();
				BcryptService.Client clientToBE = BEworker.getClient();
				if (!transportToBE.open()) {
					transportToBE.open();
				}
				// send load to BE
				BEworker.NodeIsBusy(true);
				BEworker += BEworker.addLoad(password.size(), logRounds);
				List<String> hashed = clientToBE.hashAtBE(passwords, logRounds);
				transportToBE.close();
				BEworker -= BEworker.reduceLoad(password.size(), logRounds);
				BEworker.NodeIsBusy(false);
				System.out.println("hashed password: " + hashed);
				return hashed;
			} catch (Exception e) {
				System.out.println("Hashing failed: " + e.getMessage());
				BEworker.NodeManager.findWorker();
			}
		}

	}

	public List<Boolean> checkHash(List<String> password, List<String> hashed)throws IllegalArgument, org.apache.thrift.TException {
		
		lastbatch = Instant.now();
		boolean[] result = new boolean[passwords.size()];
		try {
			int numberOfThreads = Math.min(password.size(), 4);
			int numberPerThreads = password.size() / numberOfThreads;
			CountDownLatch latch = new CountDownLatch(numberPerThreads);

			if (password.size() > 1) { // password larger than 1
				for (int i = 0; i < password.size(); ++i) {
					int firstCheck = i * numberPerThreads;
					int lastCheck;
					if (lastCheck == numberOfThreads - 1) {
						lastCheck = password.size();
					} else {
						lashHash = (i + 1) * numberPerThreads;
					}
					for (int j = firstCheck; j < lastCheck; ++j) {
						boolean checked;
						try {
							checked = BCrypt.checkpw(password.get(j), hashed.get(j));
						} catch (Exception e) {
							checked = false;
						}
						result[j] = checked;
					}
				}
				executorService.shutdown();
				executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
				System.out.println("finished hashing " + Arrays.asList(result));
				return Arrays.asList(result);
			} else {
				try {//check only one password
					List<Boolean> ret = new ArrayList<>(password.size());
					for (int i = 0; i < password.size(); ++i) {
						String onePwd = password.get(i);
						String oneHash = hashed.get(i);
						boolean checked;
						try {
							checked = BCrypt.checkpw(onePwd, oneHash);
						} catch (Exception e) {
							checked = false;
						} 
						ret.add(checked);
					}	
					return "All checked " + ret;
				} catch (Exception e) {
					System.out.println("hashing one password failed!");
					throw new IllegalArgument(e.getMessage());
				}
			}
		
		} catch (Exception e) {
			System.out.println("hashing password failed!");
			throw new IllegalArgument(e.getMessage());
		}
	}



	public List<String> offloadCheck(List<String> password, short logRounds) throws IllegalArgument, TException {
		precheckHash(password, hashed);
		// find a Node
		NodeManager.WorkStatus BEworker = NodeManager.findWorker();
		System.out.println("Find a backend Thread: " + Thread.currentThread().getName() + "Worker Found: " + BEworker);
	

		while (BEworker != null) {
			try {
				// Connect to Node
				transport = new TFramedTransport(new TSocket(host, Integer.parseInt(port)));
				client = new BcryptService.Client(new TBinaryProtocol(transport));
				TTransport transportToBE = BEworker.getTransport();
				BcryptService.Client clientToBE = BEworker.getClient();
				if (!transportToBE.open()) {
					transportToBE.open();
				}
				// send load to BE
				BEworker.NodeIsBusy(true);
				BEworker += BEworker.addLoad(password.size(), hashed);
				List<String> checkedPasswords = clientToBE.checkHash(passwords, hashed);
				transportToBE.close();
				BEworker -= BEworker.reduceLoad(password.size(), hashed);
				BEworker.NodeIsBusy(false);
				System.out.println("checked passwords: " + checkedPasswords);
				return checkedPasswords;
			} catch (Exception e) {
				System.out.println("Hashing failed: " + e.getMessage());
				BEworker.NodeManager.findWorker();
			}
		}
	
	}

	public void addBEHandler (String ipaddr, String port) throws IllegalArgument, org.apache.thrift.TException {
		System.out.println("shit");
		try {
			System.out.println("add aBEHANDLer at " + ipaddr+":"+port );
			TSocket sock = new TSocket(ipaddr, Integer.parseInt(port));
			TTransport transport = new TFramedTransport(sock);
			TProtocol protocol = new TBinaryProtocol(transport);
			BcryptService.Client client = new BcryptService.Client(protocol);
			transport.open();

			BE_client_list.add(client);
			System.out.println(BE_client_list);
		} catch (TException x) {
			x.printStackTrace();
		} 
	}


	// class hashMulti implements Runnable {
	// 	private List<String> password1;
	// 	private short logRounds1;
	// 	private String[] pwdSize1;
	// 	private int first;
	// 	private int last;
	// 	private CountDownLatch latch1;
	// }

	// @Override
	// public void run() {
	// 	hashPassword(password1, logRounds1, pwdSize1, first1, last1);
	// 	latch1.countDown();
	// }

	// public hashMulti (List<String> password, short logRounds, String[] pwdSize, int first, int last, CountDownLatch latch) {
	// 	password1 = password;
	// 	logRounds1 = logRounds;
	// 	pwdSize1 = pwdSize;
	// 	first1 = first;
	// 	last1 = last;
	// 	latch1 = latch;
	// }
}
