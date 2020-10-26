package cassdemo.backend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/*
 * For error handling done right see: 
 * https://www.datastax.com/dev/blog/cassandra-error-handling-done-right
 * 
 * Performing stress tests often results in numerous WriteTimeoutExceptions, 
 * ReadTimeoutExceptions (thrown by Cassandra replicas) and 
 * OpetationTimedOutExceptions (thrown by the client). Remember to retry
 * failed operations until success (it can be done through the RetryPolicy mechanism:
 * https://stackoverflow.com/questions/30329956/cassandra-datastax-driver-retry-policy )
 */

public class BackendSession {

	private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

	private Session session;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {

		Cluster cluster = Cluster.builder().addContactPoint(contactPoint)
			.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM))
			.build();

		try {
			session = cluster.connect(keyspace);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		prepareStatements();
	}


	private static PreparedStatement INSERT_INTO_NICKS;
	private static PreparedStatement SELECT_ALL_FROM_NICKS;
	private static PreparedStatement SELECT_NICK;
	private static PreparedStatement INSERT_STATUS;
	private static PreparedStatement DELETE_STATUS_BY_NICK;

	private static PreparedStatement SELECT_STATUS;

	private static final String NICK_FORMAT = "Nick: %-10s,Status: %-10s\n";

	private void prepareStatements() throws BackendException {
		try {
			 SELECT_ALL_FROM_NICKS = session.prepare("SELECT * FROM nicks;");
			 SELECT_NICK = session.prepare("SELECT * FROM nicks WHERE nick = ?;");
			 INSERT_INTO_NICKS = session.prepare("INSERT INTO nicks (nick,status) VALUES(?,?);");
			 INSERT_STATUS = session.prepare("UPDATE nicks SET status = ? WHERE nick = ?");
			 DELETE_STATUS_BY_NICK = session.prepare("UPDATE nicks SET status = null WHERE nick = ?");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}
		logger.info("Statements prepared");
	}

	protected void finalize() {
		try {
			if (session != null) {
				session.getCluster().close();
			}
		} catch (Exception e) {
			logger.error("Could not close existing cluster", e);
		}
	}

	public String selectAll() throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_NICKS);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			String nick = row.getString("nick");
			String status = row.getString("status");

			builder.append(String.format(NICK_FORMAT, nick,status));
		}

		return builder.toString();
	}

	public String getStatusByNick(String nick) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_NICK);
		bs.bind(nick);

		ResultSet rs = null;
		try {
			rs = session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. "+e.getMessage()+".",e);
		}
		String status = "";
		for(Row row : rs){
			status = row.getString("status");
		}

		logger.info("Status: " +status);
		return status;
	}

	public void deleteStatusByNick(String nick) throws BackendException {
		BoundStatement bs = new BoundStatement(DELETE_STATUS_BY_NICK);
		bs.bind(nick);

		try{
			session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. "+e.getMessage()+".",e);
		}

		logger.info("Status deleted for: " +nick);
	}

	public void updateStatusByNick(String nick, String status) throws BackendException{
		BoundStatement bs = new BoundStatement(INSERT_STATUS);
		bs.bind(status,nick);

		try{
			session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform an upsert. "+e.getMessage() + ".",e);
		}

		logger.info("Set "+status +"status for " + nick +" nick");
	}

	public void upsertNick(String nickName, String status) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_NICKS);
		bs.bind(nickName,status);
		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e ){
			throw new BackendException("Could not perform a query. "+ e.getMessage() +".",e);
		}
		logger.info("Nick "+nickName+" upserted");
	}


}
