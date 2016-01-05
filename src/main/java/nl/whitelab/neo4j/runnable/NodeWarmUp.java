package nl.whitelab.neo4j.runnable;

import java.io.IOException;
import java.io.Writer;

import nl.whitelab.neo4j.database.LinkLabel;
import nl.whitelab.neo4j.util.HumanReadableFormatter;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class NodeWarmUp implements Runnable {
	private final GraphDatabaseService database;
	private final Writer writer;
	private final Node startNode;
	private final long start;
	
	public NodeWarmUp(GraphDatabaseService db, 
			Writer w, 
			Node s, long st) {
		database = db;
		writer = w;
		startNode = s;
		start = st;
	}

	@Override
	public void run() {
		try (Transaction tx = database.beginTx())
        {
			writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Warming up collection "+startNode.getProperty("title")+"...\n");
			writer.flush();
			int d = 0;
			for (Relationship hasDocument : startNode.getRelationships(LinkLabel.HAS_DOCUMENT, Direction.OUTGOING)) {
				Node document = hasDocument.getOtherNode(startNode);
				
				for (String key : document.getPropertyKeys()) {
            		document.getProperty(key);
            	}
            	
            	for (Relationship hasMetadatum : document.getRelationships(LinkLabel.HAS_METADATUM, Direction.OUTGOING)) {
            		Node metadatum = hasMetadatum.getOtherNode(document);
            		metadatum.getProperty("group");
            		metadatum.getProperty("key");
            		hasMetadatum.getProperty("value");
        		}
            	
            	for (Relationship hasToken : document.getRelationships(LinkLabel.HAS_TOKEN, Direction.OUTGOING)) {
            		Node token = hasToken.getOtherNode(document);
            		
            		for (Relationship hasAnnotation : token.getRelationships(Direction.OUTGOING)) {
            			Node annotation = hasAnnotation.getOtherNode(token);
            			if (annotation.hasProperty("label"))
            				annotation.getProperty("label");
            		}
            	}
            	d++;
            	if (d % 50000 == 0) {
            		writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - "+startNode.getProperty("title")+": read "+String.valueOf(d)+" documents so far...\n");
    				writer.flush();
            	}
			}
			writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Finished warming up collection "+startNode.getProperty("title")+".\n");
			writer.flush();
			
            tx.success();
        } catch (IOException e) {
			e.printStackTrace();
		}
	}
}
