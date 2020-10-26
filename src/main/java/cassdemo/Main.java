package cassdemo;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;

public class Main {

	private static final String PROPERTIES_FILENAME = "config.properties";

	public static void main(String[] args) throws IOException, BackendException, InterruptedException {
		String contactPoint = null;
		String keyspace = null;
		Properties properties = new Properties();
		try {
			properties.load(Main.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME));

			contactPoint = properties.getProperty("contact_point");
			keyspace = properties.getProperty("keyspace");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
			
		BackendSession session = new BackendSession(contactPoint, keyspace);

		List<String> nicks = Arrays.asList("robert", "marcin", "maciej", "filip");

		String mineID = InetAddress.getLocalHost().getHostName(); // Naszym ID jest HostName komputera na którym uruchamiamy skrypt
		for(String nick : nicks){
			session.upsertNick(nick,null);
		}
		
		for (int i = 0; i < 10; i++) {
			new MyThread(session, mineID, String.valueOf(i), nicks).start();
		}
	}
}

class MyThread extends Thread {
	private BackendSession session;
	private String mineID;
	private String id;
	private List<String> nicks;

    public MyThread(BackendSession session, String mineID, String id, List<String> nicks) {
        this.session = session;
        this.mineID = mineID;
        this.id = id;
        this.nicks = nicks;
    }

    @Override
    public void run() {
        String ID = mineID + "-" + id;
		Random rand = new Random();

		while(true){
			try {
				String output = session.selectAll();
				System.out.println("Nicks{ \n"+output+"}");

				String randomNick,nickStatus;

				do{
					// 1. Wylosować jeden z 4 nicków
					randomNick = nicks.get(rand.nextInt(nicks.size()));
					// 2. Wykonac SELECT y sprawdzic czy nick jest wolny
					nickStatus = session.getStatusByNick(randomNick);
					// 3. Jesli nick jest zajety, wroc do poczatku
				}while (nickStatus != null);

				// 4. Wykonac INSERT by zajac nicka (dopisujac id swojego wezla do oddzielnej kolumny)
				session.updateStatusByNick(randomNick,ID);
				String newStatus =session.getStatusByNick(randomNick);
				// 5.Wykonac SELECT by sprawdzic czy nick jest faktycznie zajety przez nas
				output = session.selectAll();
				System.out.println("Nicks{ \n"+output+"}");
				// 6. Jeśli nick nie zostal zajety przez nas to przez kogo? Wroc do poczatku

				String nickOwnerStatus = session.getStatusByNick(randomNick);
				if(nickOwnerStatus.equals(ID)){
					System.out.println("Nickname is owned by our node");
					Thread.sleep(10);
					session.deleteStatusByNick(randomNick);
					Thread.sleep(1000);
				} else {
					System.out.println("Nickname isn't owned by us but by: "+nickOwnerStatus);
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
    }
}