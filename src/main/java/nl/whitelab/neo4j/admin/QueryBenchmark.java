package nl.whitelab.neo4j.admin;

import com.google.common.collect.Lists;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import nl.whitelab.neo4j.DatabasePlugin;
import nl.whitelab.neo4j.NodeLabel;
import nl.whitelab.neo4j.database.LinkLabel;
import nl.whitelab.neo4j.util.HumanReadableFormatter;
import nl.whitelab.neo4j.util.Query;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;

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
	
	@GET
	@Path( "/single" )
	public Response performTest(@QueryParam("pattern") final String cql) {

		final long start = System.currentTimeMillis();
		

		StreamingOutput stream = new StreamingOutput() {
			@Override
			public void write(OutputStream os) throws IOException, WebApplicationException {
				final Writer writer = new BufferedWriter(new OutputStreamWriter(os));
				
				List<String> hits = new ArrayList<String>();
				int h = 0;
				
				String[] patts = cql.split("\\]\\[");
				List<LinkLabel> linkLabels = new ArrayList<LinkLabel>();
				List<NodeLabel> nodeLabels = new ArrayList<NodeLabel>();
				List<String> patterns = new ArrayList<String>();

				Pattern pattern = Pattern.compile("\\[*(word|lemma|pos|phonetic)=\"(\\(\\?[ci]\\))*(.+)\"\\]*");
				Pattern pattern2 = Pattern.compile("[A-Z]+\\.*\\*");
				
				for (int i = 0; i < patts.length; i++) {
					Matcher matcher = pattern.matcher(patts[i]);
			        if (matcher.matches()) {
			        	String tag = matcher.group(1);
			        	String patt = matcher.group(3);
			        	if (tag.equals("pos")) {
			        		Matcher matcher2 = pattern2.matcher(patt);
			        		if (matcher2.matches())
			        			tag = "poshead";
			        	}
			        	linkLabels.add(tagToLinkLabel(tag));
			        	nodeLabels.add(tagToNodeLabel(tag));
			        	patterns.add(patt);
			        }
				}
				
				if (patterns.size() > 0 && linkLabels.size() > 0 && nodeLabels.size() > 0) {
					try ( Transaction tx = database.beginTx() ) {
						Index<Node> nodeIndex = database.index().forNodes( nodeLabels.get(0).name() );
						
						for (Node startNode : nodeIndex.query("label", patterns.get(0))) {
							Traverser t = getPatternMatch(startNode, patterns, nodeLabels, linkLabels);
							
//							List<org.neo4j.graphdb.Path> list = Lists.newArrayList(t.iterator());
//							Collections.sort(list);
							
							ResourceIterator<org.neo4j.graphdb.Path> it = t.iterator();
							while (it.hasNext()) {
								org.neo4j.graphdb.Path path = it.next();
								
								if (path.length() == patterns.size()) {
									
									Iterator<Node> nit = path.nodes().iterator();
									Iterator<Relationship> rit = path.relationships().iterator();
									
									String p = "";
									
									while (nit.hasNext()) {
										Node node = nit.next();
										String annotation = getNodeAnnotation(node, LinkLabel.HAS_TYPE);
										if (annotation.length() > 0) {
											p = p+"("+String.valueOf(node.getId())+":"+annotation+")";
											if (rit.hasNext()) {
												Relationship rel = rit.next();
												if (rel.getEndNode().equals(node))
													p = p+"<";
												p = p+"-["+rel.getType().name()+"]-";
												if (rel.getStartNode().equals(node))
													p = p+">";
											}
										} else if (rit.hasNext())
											rit.next();
									}
									h++;
									if (hits.size() < 50)
										hits.add(p);
								}
							}
						}
						tx.success();
					}
					
					writer.write("Query returned "+String.valueOf(h)+" hits. Displaying first 50:\n");
					int i = 0;
					for (String hit : hits) {
						i++;
						writer.write(String.valueOf(i)+": "+hit+"\n");
						writer.flush();
					}
				} else {
					writer.write("No pattern supplied.\n");
				}

				System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Completed query test.");
				writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Completed query test.\n");
				writer.flush();
			}
		};

		return Response.ok().entity( stream ).type( MediaType.TEXT_PLAIN ).build();
	}
	
//	private Traverser getLeftContext(final Node startNode) {
//		TraversalDescription td = database.traversalDescription()
//	            .breadthFirst()
//	            .relationships( LinkLabel.NEXT, Direction.INCOMING )
//	            .evaluator( Evaluators.excludeStartPosition() );
//	    return td.traverse( startNode );
//	}
	
	private Traverser getPatternMatch(Node startNode, final List<String> patterns, final List<NodeLabel> nodeLabels, final List<LinkLabel> linkLabels) {
		TraversalDescription td = database.traversalDescription()
			.depthFirst()
			.evaluator( new Evaluator() {
				public Evaluation evaluate( final org.neo4j.graphdb.Path path ) {
	                if ( path.length() == 0 ) {
	                    return Evaluation.EXCLUDE_AND_CONTINUE;
	                }
	                boolean isToken = path.endNode().hasLabel(NodeLabel.WordToken);
	                boolean included = isToken && (path.length() == 1 || nodeHasAnnotation(path.endNode(), linkLabels.get(path.length() - 1), patterns.get(path.length() - 1)));
	                boolean continued = path.length() < patterns.size();
	                return Evaluation.of( included, continued );
	            }
	        } )
			.relationships(LinkLabel.NEXT, Direction.OUTGOING)
			.relationships(LinkLabel.HAS_TYPE, Direction.INCOMING)
			.relationships(LinkLabel.HAS_LEMMA, Direction.INCOMING)
			.relationships(LinkLabel.HAS_POS_TAG, Direction.INCOMING)
			.relationships(LinkLabel.HAS_HEAD, Direction.INCOMING);
		return td.traverse(startNode);
	}
	
	private Boolean nodeHasAnnotation(Node node, LinkLabel link, String label) {
		return node.getSingleRelationship(link, Direction.OUTGOING).getOtherNode(node).getProperty("label").equals(label);
	}
	
	private String getNodeAnnotation(Node node, LinkLabel linkLabel) {
		if (node.hasRelationship(linkLabel, Direction.OUTGOING))
			return node.getSingleRelationship(linkLabel, Direction.OUTGOING).getOtherNode(node).getProperty("label").toString();
		return "";
	}
	
	private LinkLabel tagToLinkLabel(String tag) {
		if (tag.equals("lemma"))
			return LinkLabel.HAS_LEMMA;
		else if (tag.equals("pos"))
			return LinkLabel.HAS_POS_TAG;
		else if (tag.equals("poshead"))
			return LinkLabel.HAS_HEAD;
		else if (tag.equals("phonetic"))
			return LinkLabel.HAS_PHONETIC;
		return LinkLabel.HAS_TYPE;
	}
	
	private NodeLabel tagToNodeLabel(String tag) {
		if (tag.equals("lemma"))
			return NodeLabel.Lemma;
		else if (tag.equals("pos"))
			return NodeLabel.PosTag;
		else if (tag.equals("poshead"))
			return NodeLabel.PosHead;
		else if (tag.equals("phonetic"))
			return NodeLabel.Phonetic;
		return NodeLabel.WordType;
	}

}
