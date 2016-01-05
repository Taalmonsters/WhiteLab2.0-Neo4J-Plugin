package nl.whitelab.neo4j.admin;

import nl.whitelab.neo4j.DatabasePlugin;
import nl.whitelab.neo4j.database.LinkLabel;
import nl.whitelab.neo4j.database.NodeLabel;
import nl.whitelab.neo4j.runnable.IndexWarmUp;
import nl.whitelab.neo4j.runnable.NodeWarmUp;
import nl.whitelab.neo4j.runnable.PosCounter;
import nl.whitelab.neo4j.util.HumanReadableFormatter;
import nl.whitelab.neo4j.util.Query;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@Path("/manage")
public class DatabaseAdministration extends DatabasePlugin {

	public DatabaseAdministration(@Context GraphDatabaseService database) {
		super(database);
	}
	
	@POST
	@Path( "/cql/parse" )
	public Response cqlToCypher( 
			@DefaultValue("document") @QueryParam("within") final String within, 
			@QueryParam("pattern") final String cqlPattern,
			@QueryParam("filter") final String filter ) {

		System.out.println("DatabaseAdministration.cqlToCypher");
		Query query = constructor.parseQuery(null, within, null, cqlPattern, filter, null, null, null, null, null, 1, 50, 0, 5, false);
		if (query != null) {
			return Response.ok().entity( query.toCypher() ).type( MediaType.TEXT_PLAIN ).build();
		}
		return Response.ok().entity( "error: No query supplied." ).type( MediaType.TEXT_PLAIN ).build();
	}
	
	@POST
	@Path( "/metadata/count" )
	public Response countMetadata() {
		final long start = System.currentTimeMillis();

		StreamingOutput stream = new StreamingOutput() {
			@Override
			public void write(OutputStream os) throws IOException, WebApplicationException {
				final Writer writer = new BufferedWriter(new OutputStreamWriter(os));
				System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Counting metadata documents and values...");
				writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Counting metadata documents and values...\n");
				writer.flush();
				List<String> corpora = new ArrayList<String>();

				try ( Transaction ignored = database.beginTx() ) {
					ResourceIterator<Node> corpIt = database.findNodes(NodeLabel.Corpus);
					
		            while (corpIt.hasNext()) {
		            	Node corpus = corpIt.next();
		            	corpora.add((String) corpus.getProperty("label"));
		            }
				}

				for (String corpus : corpora) {
					String query = "MATCH (m:Metadatum) "
							+ "OPTIONAL MATCH (m)<-[:HAS_METADATUM]-(d:Document{corpus_label:'"+corpus+"'}) "
							+ "RETURN m AS metadatum, CASE WHEN COUNT(DISTINCT d) > 0 THEN COUNT(DISTINCT d) ELSE 0 END AS document_count";
					System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - QUERY: "+query);
					try ( Transaction tx = database.beginTx(); Result result = database.execute(query) ) {
						Node metadatum = null;
						Long docCount = (long) -1;
						while ( result.hasNext() ) {
							Map<String,Object> row = result.next();
							for ( Entry<String, Object> column : row.entrySet() ) {
								String field = column.getKey();
								if (field.equals("metadatum"))
									metadatum = (Node) column.getValue();
								else
									docCount = (Long) column.getValue();
							}
							if (metadatum != null && docCount > -1 ) {
								metadatum.setProperty("document_count_"+corpus, docCount);
								String metaLabel = (String) metadatum.getProperty("label");
								System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - processed document count for "+metaLabel+" in corpus "+corpus+"...");
								writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - processed document count for "+metaLabel+" in corpus "+corpus+"...\n");
								writer.flush();
							}
						}
						tx.success();
					}
				}
				
				String query = "MATCH (m:Metadatum)<-[v:HAS_METADATUM]-(:Document) RETURN m AS metadatum, COUNT(DISTINCT v.value) AS value_count";
				System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - QUERY: "+query);
				try ( Transaction tx = database.beginTx(); Result result = database.execute(query) ) {
					Node metadatum = null;
					Long valCount = (long) -1;
					while ( result.hasNext() ) {
						Map<String,Object> row = result.next();
						for ( Entry<String, Object> column : row.entrySet() ) {
							String field = column.getKey();
							if (field.equals("metadatum"))
								metadatum = (Node) column.getValue();
							else
								valCount = (Long) column.getValue();
						}
						if (metadatum != null && valCount > -1 ) {
							metadatum.setProperty("value_count", valCount);
							String metaLabel = (String) metadatum.getProperty("label");
							System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - processed value count for "+metaLabel+"...");
							writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - processed value count for "+metaLabel+"...\n");
							writer.flush();
						}
					}
					tx.success();
				}

				System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Completed metadata count.");
				writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Completed metadata count.\n");
				writer.flush();
			}
		};
		return Response.ok().entity( stream ).type( MediaType.TEXT_PLAIN ).build();
	}
	
	@POST
	@Path( "/pos/count" )
	public Response countPos() {
		final long start = System.currentTimeMillis();

		StreamingOutput stream = new StreamingOutput() {
			@Override
			public void write(OutputStream os) throws IOException, WebApplicationException {
				final Writer writer = new BufferedWriter(new OutputStreamWriter(os));
				System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Counting pos tokens...");
				writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Counting pos tokens...\n");
				writer.flush();
//				List<String> corpora = new ArrayList<String>();
				
				System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - resetting pos counts...");
				writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - resetting pos counts...\n");
				writer.flush();
				String query = "MATCH (p:PosTag) SET p.token_count_CGN = 0";

				try ( Transaction ignored = database.beginTx(); Result result = database.execute(query); ) {
					System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - finished resetting pos counts.");
					writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - finished resetting pos counts.\n");
					writer.flush();
					ResourceIterator<Node> corpIt = database.findNodes(NodeLabel.Corpus);
					
		            while (corpIt.hasNext()) {
		            	Node corpus = corpIt.next();
		            	String title = (String) corpus.getProperty("title");
		            	corpus.setProperty("title", title.replaceAll("-", "_"));
						List<Thread> threads = new ArrayList<Thread>();
		            	
		            	for (Relationship hasCollection : corpus.getRelationships(LinkLabel.HAS_COLLECTION, Direction.OUTGOING)) {
		            		Node collection = hasCollection.getOtherNode(corpus);
		            		Thread colCountThread = new Thread(new PosCounter(database, writer, collection, start));
		            		colCountThread.start();
		            		threads.add(colCountThread);
		            		if (threads.size() == 16)
		            			threads = finishThreads(threads);
		            	}
		            	
		            	finishThreads(threads);
		            }
		            ignored.success();
				}

//				List<Thread> threads = new ArrayList<Thread>();
//				
//				String query = "MATCH (p:PosTag) RETURN DISTINCT p";
//				try ( Transaction tx = database.beginTx(); Result result = database.execute(query) ) {
//					while ( result.hasNext() ) {
//						Map<String,Object> row = result.next();
//						for ( Entry<String, Object> column : row.entrySet() ) {
//							Node posTag = (Node) column.getValue();
//							for (String corpus : corpora) {
//								Thread posCountThread = new Thread(new PosCounter(database, writer, posTag, corpus, start));
//			            		posCountThread.start();
//			            		threads.add(posCountThread);
//			            		if (threads.size() == 16)
//			            			threads = finishThreads(threads);
////								countPosTagTokens(posTag, corpus);
////								System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - processed "+posTagLabel+" in "+corpus+"...");
////								writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - processed "+posTagLabel+" in "+corpus+"...\n");
////								writer.flush();
//							}
//						}
//					}
//				}
				
//            	finishThreads(threads);
				
				
//				for (String corpus : corpora) {
//					String query = "MATCH (p:PosTag) "
//							+ "OPTIONAL MATCH p<-[:HAS_POS_TAG]-(w:WordToken)<-[:HAS_TOKEN]-(d:Document{corpus_title:'"+corpus+"'}) "
//							+ "RETURN p AS postag, CASE WHEN COUNT(DISTINCT w) > 0 THEN COUNT(DISTINCT w) ELSE 0 END AS token_count";
//					System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - QUERY: "+query);
//					try ( Transaction tx = database.beginTx(); Result result = database.execute(query) ) {
//						Node postag = null;
//						Long tokCount = (long) -1;
//						while ( result.hasNext() ) {
//							Map<String,Object> row = result.next();
//							for ( Entry<String, Object> column : row.entrySet() ) {
//								String field = column.getKey();
//								if (field.equals("postag"))
//									postag = (Node) column.getValue();
//								else
//									tokCount = (Long) column.getValue();
//							}
//							if (postag != null && tokCount > -1 ) {
//								postag.setProperty("token_count_"+corpus, tokCount);
//								String posLabel = (String) postag.getProperty("label");
//								System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - processed token count for "+posLabel+" in corpus "+corpus+"...");
//								writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - processed token count for "+posLabel+" in corpus "+corpus+"...\n");
//								writer.flush();
//							}
//						}
//						tx.success();
//					}
//				}

				System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Completed pos token count.");
				writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Completed pos token count.\n");
				writer.flush();
			}

//			private void countPosTagTokens(Node posTag, String corpus) {
//				String posTagLabel = (String) posTag.getProperty("label");
//				String query = "MATCH (p:PosTag{label:'"+posTagLabel+"'})<-[:HAS_POS_TAG]-(t:WordToken)<-[:HAS_TOKEN]-(:Document{corpus_title:'"+corpus+"'}) "
//						+ "RETURN COUNT(DISTINCT t.xmlid) AS token_count";
//				try ( Transaction tx = database.beginTx(); Result result = database.execute(query) ) {
//					Long tokenCount = (long) result.next().entrySet().iterator().next().getValue();
//					posTag.setProperty("token_count", tokenCount);
//					tx.success();
//				}
//			}
		};
		return Response.ok().entity( stream ).type( MediaType.TEXT_PLAIN ).build();
	}
	
	@POST
	@Path( "/pos/oldcount" )
	public Response countPosOld() {
		final long start = System.currentTimeMillis();

		StreamingOutput stream = new StreamingOutput() {
			@Override
			public void write(OutputStream os) throws IOException, WebApplicationException {
				final Writer writer = new BufferedWriter(new OutputStreamWriter(os));
				System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Counting pos tokens...");
				writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Counting pos tokens...\n");
				writer.flush();
				List<String> corpora = new ArrayList<String>();
				
				String query = "MATCH (n:PosFeature)<-[:HAS_FEATURE]-(p:PosTag) WITH n, COUNT(DISTINCT p) AS c SET n.pos_tag_count = c";
				
				try (Transaction tx = database.beginTx(); Result result = database.execute(query)) {

					ResourceIterator<Node> corpIt = database.findNodes(NodeLabel.Corpus);
					
		            while (corpIt.hasNext()) {
		            	Node corpus = corpIt.next();
		            	corpora.add((String) corpus.getProperty("title"));
		            }
		            
					ResourceIterator<Node> posTagIt = database.findNodes(NodeLabel.PosTag);
					
					while (posTagIt.hasNext()) {
						Node posTag = posTagIt.next();
						String posLabel = (String) posTag.getProperty("label");
						Map<String,Integer> countsByCorpus = new HashMap<String,Integer>();
						
						for (Relationship hasPosTag : posTag.getRelationships(LinkLabel.HAS_POS_TAG, Direction.INCOMING)) {
							Node wordToken = hasPosTag.getOtherNode(posTag);
							Node document = wordToken.getRelationships(LinkLabel.HAS_TOKEN, Direction.INCOMING).iterator().next().getOtherNode(wordToken);
							String corpusTitle = (String) document.getProperty("corpus_title");
							
							if (!countsByCorpus.containsKey(corpusTitle))
								countsByCorpus.put(corpusTitle, 0);
							countsByCorpus.put(corpusTitle, countsByCorpus.get(corpusTitle)+1);
						}

						for (String corpusTitle : corpora)
							if (countsByCorpus.containsKey(corpusTitle))
								posTag.setProperty("token_count_"+corpusTitle, countsByCorpus.get(corpusTitle));
							else
								posTag.setProperty("token_count_"+corpusTitle, 0);
						System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - processed "+posLabel+"...");
						writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - processed "+posLabel+"...\n");
						
					}
					
		            tx.success();
		        }
				
//				try (Transaction tx = database.beginTx()) {
//		            
//					ResourceIterator<Node> posHeadIt = database.findNodes(NodeLabel.PosHead);
//					
//					while (posHeadIt.hasNext()) {
//						Node posHead = posHeadIt.next();
//						Map<String,List<String>> countsByCorpus = new HashMap<String,List<String>>();
//						
//						for (Relationship hasPosHead : posHead.getRelationships(LinkLabel.HAS_HEAD, Direction.INCOMING)) {
//							Node posTag = hasPosHead.getOtherNode(posHead);
//							for (Relationship hasPosTag : posTag.getRelationships(LinkLabel.HAS_POS_TAG, Direction.INCOMING)) {
//								Node wordToken = hasPosTag.getOtherNode(posTag);
//								Node document = wordToken.getRelationships(LinkLabel.HAS_TOKEN, Direction.INCOMING).iterator().next().getOtherNode(wordToken);
//								String corpusTitle = (String) document.getProperty("corpus_title");
//								String xmlid = (String) wordToken.getProperty("xmlid");
//								
//								if (!countsByCorpus.containsKey(corpusTitle))
//									countsByCorpus.put(corpusTitle, new ArrayList<String>());
//								if (!countsByCorpus.get(corpusTitle).contains(xmlid))
//									countsByCorpus.get(corpusTitle).add(xmlid);
//								
//							}
//						}
//
//						for (String corpusTitle : corpora)
//							if (countsByCorpus.containsKey(corpusTitle))
//								posHead.setProperty("document_count_"+corpusTitle, countsByCorpus.get(corpusTitle).size());
//							else
//								posHead.setProperty("document_count_"+corpusTitle, 0);
//						
//					}
//					
//		            tx.success();
//		        }
//				
//				try (Transaction tx = database.beginTx()) {
//		            
//					ResourceIterator<Node> posFeatIt = database.findNodes(NodeLabel.PosFeature);
//					
//					while (posFeatIt.hasNext()) {
//						Node posFeat = posFeatIt.next();
//						Map<String,List<String>> countsByCorpus = new HashMap<String,List<String>>();
//						
//						for (Relationship hasPosFeat : posFeat.getRelationships(LinkLabel.HAS_FEATURE, Direction.INCOMING)) {
//							Node posTag = hasPosFeat.getOtherNode(posFeat);
//							for (Relationship hasPosTag : posTag.getRelationships(LinkLabel.HAS_POS_TAG, Direction.INCOMING)) {
//								Node wordToken = hasPosTag.getOtherNode(posTag);
//								Node document = wordToken.getRelationships(LinkLabel.HAS_TOKEN, Direction.INCOMING).iterator().next().getOtherNode(wordToken);
//								String corpusTitle = (String) document.getProperty("corpus_title");
//								String xmlid = (String) wordToken.getProperty("xmlid");
//								
//								if (!countsByCorpus.containsKey(corpusTitle))
//									countsByCorpus.put(corpusTitle, new ArrayList<String>());
//								if (!countsByCorpus.get(corpusTitle).contains(xmlid))
//									countsByCorpus.get(corpusTitle).add(xmlid);
//								
//							}
//						}
//
//						for (String corpusTitle : corpora)
//							if (countsByCorpus.containsKey(corpusTitle))
//								posFeat.setProperty("document_count_"+corpusTitle, countsByCorpus.get(corpusTitle).size());
//							else
//								posFeat.setProperty("document_count_"+corpusTitle, 0);
//						
//					}
//					
//		            tx.success();
//		        }

				System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Completed pos token count.");
				writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Completed pos token count.\n");
				writer.flush();
			}
		};
		return Response.ok().entity( stream ).type( MediaType.TEXT_PLAIN ).build();
	}

	@POST
	@Path( "/warmup" )
	public Response warmup() {
		final long start = System.currentTimeMillis();

		StreamingOutput stream = new StreamingOutput() {
			@Override
			public void write(OutputStream os) throws IOException, WebApplicationException {
				final Writer writer = new BufferedWriter(new OutputStreamWriter(os));
				System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Warming up database...");
				writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Warming up database...\n");
				writer.flush();
				
				try (Transaction tx = database.beginTx()) {
					ResourceIterator<Node> corpIt = database.findNodes(NodeLabel.Corpus);
					
		            while (corpIt.hasNext()) {
		            	Node corpus = corpIt.next();
						List<Thread> threads = new ArrayList<Thread>();
		            	
		            	for (Relationship hasCollection : corpus.getRelationships(LinkLabel.HAS_COLLECTION, Direction.OUTGOING)) {
		            		Node collection = hasCollection.getOtherNode(corpus);
		            		Thread colWarmupThread = new Thread(new NodeWarmUp(database, writer, collection, start));
		            		colWarmupThread.start();
		            		threads.add(colWarmupThread);
		            		if (threads.size() == 16)
		            			threads = finishThreads(threads);
		            	}
		            	
		            	finishThreads(threads);
		            }
					
		            tx.success();
		        }

				System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Completed database warmup.");
				writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Completed database warmup.\n");
				writer.flush();
			}
		};
		return Response.ok().entity( stream ).type( MediaType.TEXT_PLAIN ).build();
	}
	
	private List<Thread> finishThreads(List<Thread> threads) {
		for (Thread thread : threads) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return new ArrayList<Thread>();
	}

	@POST
	@Path( "/index/warmup" )
	public Response warmupIndex() {
		final long start = System.currentTimeMillis();

		StreamingOutput stream = new StreamingOutput() {
			@Override
			public void write(OutputStream os) throws IOException, WebApplicationException {
				final Writer writer = new BufferedWriter(new OutputStreamWriter(os));
				System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Warming up indexes...");
				writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Warming up indexes...\n");
				writer.flush();
				
				try (Transaction tx = database.beginTx()) {
					NodeLabel[] labels = new NodeLabel[]{NodeLabel.WordType, NodeLabel.Lemma, NodeLabel.PosTag, NodeLabel.Phonetic};
					Map<NodeLabel, Thread> threads = new HashMap<NodeLabel, Thread>();
					
					for (NodeLabel label : labels) {
						Thread indexWarmupThread = new Thread(new IndexWarmUp(database, writer, label, start));
						indexWarmupThread.start();
						threads.put(label, indexWarmupThread);
	            		if (threads.size() == 16)
	            			threads = finishThreads(threads);
					}
	            	
	            	finishThreads(threads);
					
		            tx.success();
		        }

				System.out.println(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Completed index warmup.");
				writer.write(HumanReadableFormatter.humanReadableTimeElapsed(System.currentTimeMillis() - start)+" - Completed index warmup.\n");
				writer.flush();
			}
		};
		return Response.ok().entity( stream ).type( MediaType.TEXT_PLAIN ).build();
	}
	
	private Map<NodeLabel, Thread> finishThreads(Map<NodeLabel, Thread> threads) {
		for (NodeLabel label : threads.keySet()) {
			Thread thread = threads.get(label);
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return new HashMap<NodeLabel, Thread>();
	}
}
