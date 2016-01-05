package nl.whitelab.neo4j.runnable;

import java.io.IOException;
import java.io.Writer;

import nl.whitelab.neo4j.database.NodeLabel;
import nl.whitelab.neo4j.util.HumanReadableFormatter;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

public class IndexWarmUp implements Runnable {
	private final GraphDatabaseService database;
	private final Writer writer;
	private final NodeLabel nodeLabel;
	private final long start;
	
	public IndexWarmUp(GraphDatabaseService db, 
			Writer w, 
			NodeLabel l, long st) {
		database = db;
		writer = w;
		nodeLabel = l;
		start = st;
	}

	@Override
	public void run() {
		try (Transaction tx = database.beginTx())
        {
			writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Warming up "+nodeLabel.toString()+" index...\n");
			writer.flush();
			int d = 0;
			
			ResourceIterator<Node> it = database.findNodes(nodeLabel);
			
            while (it.hasNext()) {
            	Node node = it.next();
//            	Boolean has_token_count = false;
            	for (String prop : node.getPropertyKeys()) {
            		node.getProperty(prop);
            	}
            	
            	for (Relationship in : node.getRelationships(Direction.INCOMING)) {
            		@SuppressWarnings("unused")
					Node other = in.getOtherNode(node);
//                	for (String prop : other.getPropertyKeys()) {
//                		other.getProperty(prop);
//                	}
            	}
            	d++;
            	if (d % 50000 == 0) {
            		writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - "+nodeLabel.toString()+": read "+String.valueOf(d)+" nodes so far...\n");
    				writer.flush();
            	}
            }
			
			writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Finished warming up "+nodeLabel.toString()+" index.\n");
			writer.flush();
			
            tx.success();
        } catch (IOException e) {
			e.printStackTrace();
		}
	}
}
