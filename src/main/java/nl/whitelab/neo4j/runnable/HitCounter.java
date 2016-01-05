package nl.whitelab.neo4j.runnable;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class HitCounter implements Runnable {
	private final GraphDatabaseService database;
	private final String query;
	private long start;
	private long end;
	private int offset = 0;
	private final int number = 500;
	private long count;
	private String status = "NEW";
	
	public HitCounter(GraphDatabaseService db, String q) {
		database = db;
		query = q;
		start = System.currentTimeMillis();
	}
	

	@Override
	public void run() {
		updateStatus("RUNNING");
		while (!isDone()) {
			String q = query + " SKIP "+String.valueOf(offset)+" ";
			if (number > 0)
				q = query + "LIMIT "+String.valueOf(number);
			try ( Transaction ignored = database.beginTx(); Result result = database.execute(q) ) {
				if ( result.hasNext() ) {
				} else
					updateStatus("DONE");
			}
		}
	}
	
	private void updateStatus(String st) {
		status = st;
		if (isDone())
			end = System.currentTimeMillis();
	}
	
	public Boolean isDone() {
		if (status.equals("DONE"))
			return true;
		return false;
	}
	
	public Long getCount() {
		return count;
	}
	
	public Long getDuration() {
		return end - start;
	}

}
