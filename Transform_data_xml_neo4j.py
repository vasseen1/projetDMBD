#!/usr/bin/env python3
"""
Script pour transformer LitCovid BioC-XML vers Neo4j
Usage: python xml_to_neo4j.py litcovid2BioCXML.xml
"""

import xml.etree.ElementTree as ET
from neo4j import GraphDatabase
import sys
import time

class Neo4jLoader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def create_constraints(self):
        """Créer les contraintes et index"""
        with self.driver.session() as session:
            # Contrainte d'unicité sur PMID
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (d:Document) REQUIRE d.pmid IS UNIQUE")
            print("✓ Contraintes créées")
    
    def load_document(self, pmid, title, abstract, references):
        """Charger un document et ses références dans Neo4j"""
        with self.driver.session() as session:
            # Créer le document principal
            session.run("""
                MERGE (d:Document {pmid: $pmid})
                SET d.title = $title,
                    d.abstract = $abstract,
                    d.full_text = $title + ' ' + $abstract
                """, pmid=pmid, title=title, abstract=abstract)
            
            # Créer les relations vers les références
            if references:
                session.run("""
                    MATCH (d:Document {pmid: $pmid})
                    UNWIND $refs AS ref_pmid
                    MERGE (r:Document {pmid: ref_pmid})
                    MERGE (d)-[:REFERENCES]->(r)
                    """, pmid=pmid, refs=references)
    
    def batch_load_documents(self, documents, batch_size=1000):
        """Charger les documents par batch pour performance"""
        with self.driver.session() as session:
            session.run("""
                UNWIND $docs AS doc
                MERGE (d:Document {pmid: doc.pmid})
                SET d.title = doc.title,
                    d.abstract = doc.abstract,
                    d.full_text = doc.title + ' ' + doc.abstract
                """, docs=documents)
            
            print(f"✓ Chargé {len(documents)} documents")

def parse_litcovid_xml(xml_file):
    """Parser le fichier XML et extraire les données"""
    print(f"Lecture du fichier XML: {xml_file}")
    
    context = ET.iterparse(xml_file, events=('start', 'end'))
    
    documents = []
    document_count = 0
    current_doc = None
    
    for event, elem in context:
        if event == 'start' and elem.tag == 'document':
            current_doc = {
                'pmid': None,
                'title': '',
                'abstract_parts': [],
                'references': []
            }
        
        elif event == 'end' and elem.tag == 'document':
            if current_doc and current_doc['pmid']:
                # Construire l'abstract complet
                abstract = ' '.join(current_doc['abstract_parts'])
                
                documents.append({
                    'pmid': current_doc['pmid'],
                    'title': current_doc['title'],
                    'abstract': abstract,
                    'references': current_doc['references']
                })
                
                document_count += 1
                if document_count % 1000 == 0:
                    print(f"Documents parsés: {document_count}")
            
            elem.clear()
            current_doc = None
        
        elif event == 'end' and elem.tag == 'passage' and current_doc:
            infons = {inf.get('key'): inf.text for inf in elem.findall('infon')}
            
            # Extraire PMID
            if 'article-id_pmid' in infons:
                current_doc['pmid'] = infons['article-id_pmid']
            
            # Extraire titre
            if infons.get('section_type') == 'TITLE':
                text_elem = elem.find('text')
                if text_elem is not None and text_elem.text:
                    current_doc['title'] = text_elem.text.strip()
            
            # Extraire abstract
            elif (infons.get('section_type') == 'ABSTRACT' and 
                  infons.get('type') == 'abstract'):
                text_elem = elem.find('text')
                if text_elem is not None and text_elem.text:
                    current_doc['abstract_parts'].append(text_elem.text.strip())
            
            # Extraire références
            elif infons.get('section_type') == 'REF':
                ref_pmid = infons.get('pub-id_pmid')
                if ref_pmid:
                    current_doc['references'].append(ref_pmid)
    
    print(f"\n✓ Total de documents parsés: {document_count}")
    return documents

def load_to_neo4j(documents, uri="bolt://localhost:7687", user="neo4j", password="password"):
    """Charger les données dans Neo4j"""
    print(f"\nConnexion à Neo4j: {uri}")
    loader = Neo4jLoader(uri, user, password)
    
    try:
        # Créer les contraintes
        loader.create_constraints()
        
        # Charger les documents par batch
        batch_size = 1000
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i+batch_size]
            loader.batch_load_documents(batch, batch_size)
        
        # Créer les relations de références (séparément pour performance)
        print("\nCréation des relations de références...")
        with loader.driver.session() as session:
            refs_data = [
                {'pmid': doc['pmid'], 'refs': doc['references']}
                for doc in documents if doc['references']
            ]
            
            for i in range(0, len(refs_data), batch_size):
                batch = refs_data[i:i+batch_size]
                session.run("""
                    UNWIND $data AS item
                    MATCH (d:Document {pmid: item.pmid})
                    UNWIND item.refs AS ref_pmid
                    MERGE (r:Document {pmid: ref_pmid})
                    MERGE (d)-[:REFERENCES]->(r)
                    """, data=batch)
                print(f"✓ Créé relations pour {len(batch)} documents")
        
        print("\n✓ Chargement terminé!")
        
    finally:
        loader.close()

def main():
    start_time = time.time()
    if len(sys.argv) < 2:
        print("Usage: python xml_to_neo4j.py <input_xml> [neo4j_uri] [user] [password]")
        print("\nExemple:")
        print("  python xml_to_neo4j.py litcovid2BioCXML.xml")
        print("  python xml_to_neo4j.py litcovid2BioCXML.xml bolt://localhost:7687 neo4j mypassword")
        sys.exit(1)
    
    xml_file = sys.argv[1]
    uri = sys.argv[2] if len(sys.argv) > 2 else "bolt://localhost:7687"
    user = sys.argv[3] if len(sys.argv) > 3 else "neo4j"
    password = sys.argv[4] if len(sys.argv) > 4 else "password"
    
    # Parser le XML
    documents = parse_litcovid_xml(xml_file)
    
    # Charger dans Neo4j
    load_to_neo4j(documents, uri, user, password)

    end_time = time.time()
    elapsed = end_time - start_time

    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    print(f"\n Temps total d'exécution : {minutes} min {seconds} sec")


if __name__ == '__main__':
    main()