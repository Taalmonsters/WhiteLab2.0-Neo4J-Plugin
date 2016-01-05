package nl.whitelab.neo4j.admin;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import nl.whitelab.neo4j.DatabasePlugin;
import nl.whitelab.neo4j.util.HumanReadableFormatter;
import nl.whitelab.neo4j.util.Query;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

@Path("/test")
public class QueryBenchmark extends DatabasePlugin {

	public QueryBenchmark(@Context GraphDatabaseService database) {
		super(database);
	}

	@POST
	@Path( "/query" )
	public Response testQuery(
			@QueryParam("pattern") final String cql, 
			@DefaultValue("document") @QueryParam("within") final String within, 
			@DefaultValue("5") @QueryParam("csize") final Integer contextSize, 
			@DefaultValue("50") @QueryParam("number") final Integer number, 
			@DefaultValue("0") @QueryParam("offset") final Integer offset, 
			@DefaultValue("1") @QueryParam("view") final Integer view, 
			@QueryParam("filter") final String filter,
			@QueryParam("group") final String group,
			@QueryParam("docpid") final String docPid, 
			@DefaultValue("false") @QueryParam("count") final Boolean count, 
			@DefaultValue("1000") @QueryParam("iter") final Integer iterations) {

		final long start = System.currentTimeMillis();
		

		StreamingOutput stream = new StreamingOutput() {
			@Override
			public void write(OutputStream os) throws IOException, WebApplicationException {
				final Writer writer = new BufferedWriter(new OutputStreamWriter(os));

				if (cql == null) {
					System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - No query defined.");
					writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - No query defined.\n");
					writer.flush();
				} else if (group == null && (view == 8 || view == 16)) {
					System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - No group defined.");
					writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - No group defined.\n");
					writer.flush();
				} else if (view != 1 && view != 2 && view != 8 && view != 16) {
					System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Invalid value for view.");
					writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Invalid value for view.\n");
					writer.flush();
				} else {
					System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - CQL: "+cql+"...");
					writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - CQL: "+cql+"\n");
					writer.flush();

					Query query = constructor.parseQuery(null, within, null, cql, filter, group, null, null, null, docPid, view, number, offset, contextSize, count);
//					Query query = new Query(null, cql, false);
					
					System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Cypher: "+query.toCypher()+"...");
					writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Cypher: "+query.toCypher()+"\n");
					writer.flush();

					List<Long> durations = new ArrayList<Long>();
					boolean done = false;
					int i = 0;

					while (!done) {
						i++;
						long qstart = System.currentTimeMillis();
						try ( Transaction ignored = database.beginTx(); Result result = database.execute(query.toCypher()) ) {
							long duration = System.currentTimeMillis() - qstart;
							durations.add(duration);
//							if (i % 100 == 0) {
								Double avg = calculateAverage(durations);
//								System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Average after "+String.valueOf(i)+" iterations: "+String.valueOf(avg)+" ms.");
								writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Average after "+String.valueOf(i)+" iterations: "+String.valueOf(avg)+" ms.\n");
								writer.flush();
//							}
//							Double avg = calculateAverage(durations);
							if ((avg > 6000 && i >= 25) || i == iterations)
								done = true;
						}
					}

					Double avg = calculateAverage(durations);
					System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - ********************************************");
					writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - ********************************************\n");
					System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Completed query test. "+String.valueOf(i)+" iterations: "+String.valueOf(avg)+" ms.");
					writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Completed query test. "+String.valueOf(i)+" iterations: "+String.valueOf(avg)+" ms.\n");
					writer.flush();
				}
			}
		};

		return Response.ok().entity( stream ).type( MediaType.TEXT_PLAIN ).build();
	}

	private double calculateAverage(List<Long> vals) {
		Long sum = (long) 0;
		if(!vals.isEmpty()) {
			for (Long val : vals) {
				sum += val;
			}
			return sum.doubleValue() / vals.size();
		}
		return sum;
	}

}
