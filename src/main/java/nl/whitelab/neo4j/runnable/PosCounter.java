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

public class PosCounter implements Runnable {
	private final GraphDatabaseService database;
	private final Writer writer;
	private final Node startNode;
	private final long start;
	
	public PosCounter(GraphDatabaseService db, 
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
			writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Counting pos in collection "+startNode.getProperty("title")+"...\n");
			writer.flush();
			int d = 0;
			Node corpus = startNode.getRelationships(LinkLabel.HAS_COLLECTION, Direction.INCOMING).iterator().next().getOtherNode(startNode);
			String corpusTitle = (String) corpus.getProperty("title");
			corpusTitle = corpusTitle.replaceAll("-", "_");
			
			for (Relationship hasDocument : startNode.getRelationships(LinkLabel.HAS_DOCUMENT, Direction.OUTGOING)) {
				Node document = hasDocument.getOtherNode(startNode);
            	
            	for (Relationship hasToken : document.getRelationships(LinkLabel.HAS_TOKEN, Direction.OUTGOING)) {
            		Node token = hasToken.getOtherNode(document);
            		
            		for (Relationship hasPosTag : token.getRelationships(LinkLabel.HAS_POS_TAG, Direction.OUTGOING)) {
            			Node posTag = hasPosTag.getOtherNode(token);
            			if (posTag.hasProperty("token_count_"+corpusTitle))
            				posTag.setProperty("token_count_"+corpusTitle, (long) posTag.getProperty("token_count_"+corpusTitle) + 1);
            			else
            				posTag.setProperty("token_count_"+corpusTitle, (long) 1);
            		}
            	}
            	d++;
            	if (d % 50000 == 0) {
            		writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - "+startNode.getProperty("title")+": read "+String.valueOf(d)+" documents so far...\n");
    				writer.flush();
            	}
			}
			writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Finished counting pos in collection "+startNode.getProperty("title")+".\n");
			writer.flush();
			
            tx.success();
        } catch (IOException e) {
			e.printStackTrace();
		}
	}
}
