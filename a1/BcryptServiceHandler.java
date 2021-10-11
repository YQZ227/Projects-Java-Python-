import java.util.ArrayList;
import java.util.List;

import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {

	public boolean has_BEnodes;
	
	public BcryptServiceHandler (boolean hasNode) {
		has_BEnodes = hasNode;
	}

	public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
		if (password.size() == 0) {
			throw new IllegalArgument ("The password is empty");
		}
		if (logRounds < 4 || logRounds > 16) {
			throw new IllegalArgument ("logRounds should between 4 and 16");
		}
		String[] result = new String [password.size()];
		

		try {
			List<String> ret = new ArrayList<>();
			String onePwd = password.get(0);
			String oneHash = BCrypt.hashpw(onePwd, BCrypt.gensalt(logRounds));
			ret.add(oneHash);
			return ret;
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}

	public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
	{
		try {
			List<Boolean> ret = new ArrayList<>();
			String onePwd = password.get(0);
			String oneHash = hash.get(0);
			ret.add(BCrypt.checkpw(onePwd, oneHash));
			return ret;
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}
}
