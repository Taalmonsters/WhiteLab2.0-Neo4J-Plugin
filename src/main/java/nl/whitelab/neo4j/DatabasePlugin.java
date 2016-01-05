package nl.whitelab.neo4j;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import nl.whitelab.neo4j.cypher.CypherConstructor;
import nl.whitelab.neo4j.cypher.CypherExecutor;
import nl.whitelab.neo4j.util.Query;

import org.neo4j.graphdb.GraphDatabaseService;

public abstract class DatabasePlugin {
	protected final GraphDatabaseService database;
	protected final CypherConstructor constructor;
	protected final CypherExecutor executor;

	public DatabasePlugin(@Context GraphDatabaseService database) {
		this.database = database;
		this.constructor = new CypherConstructor(this);
		this.executor = new CypherExecutor(database);
	}
	
	protected Response getQueryResult(final Query query) {
		return executor.getQueryResult(query);
	}
	
	public CypherExecutor getExecutor() {
		return executor;
	}
	
	public GraphDatabaseService getDatabase() {
		return database;
	}

}
