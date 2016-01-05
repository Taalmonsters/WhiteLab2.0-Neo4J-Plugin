package nl.whitelab.neo4j.database;

import org.neo4j.graphdb.Label;

public enum NodeLabel implements Label { NodeCounter, Corpus, Collection, Document, Metadatum, WordToken, WordType, Lemma, PosTag, PosHead, PosFeature, Phonetic, SentenceStart, SentenceEnd, ParagraphStart, ParagraphEnd }