#!/usr/bin/env python3
"""
Script pour exporter les résultats depuis Neo4j vers fichiers TXT
Usage: python neo4j_export.py
"""

from neo4j import GraphDatabase
import sys
import time

class Neo4jExporter:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="password"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def export_documents_with_text(self, output_file):
        start_time = time.time()
        """
        Exporter PMID/titre+abstract
        Équivalent à la question 3.1
        """
        print(f"Export des documents vers {output_file}...")
        
        query = """
        MATCH (d:Document)
        WHERE d.pmid IS NOT NULL AND d.full_text IS NOT NULL
        RETURN d.pmid AS pmid, d.full_text AS text
        ORDER BY d.pmid
        """
        
        with self.driver.session() as session:
            results = session.run(query)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                count = 0
                for record in results:
                    line = f"{record['pmid']}/{record['text']}\n"
                    f.write(line)
                    count += 1
                    
                    if count % 10000 == 0:
                        print(f"  Exporté {count} documents...")
            
            print(f"✓ {count} documents exportés dans {output_file}")
        
        end_time = time.time()
        elapsed = end_time - start_time

        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)

        print(f"\n Temps total de génération du premier fichier : {minutes} min {seconds} sec")
    
    def export_documents_with_references(self, output_file):
        start_time = time.time()

        """
        Exporter PMID/ref1/ref2/ref3/...
        Équivalent à la question 3.2
        """
        print(f"\nExport des références vers {output_file}...")
        
        query = """
        MATCH (d:Document)
        WHERE d.pmid IS NOT NULL
        OPTIONAL MATCH (d)-[:REFERENCES]->(r:Document)
        WITH d.pmid AS pmid, collect(r.pmid) AS refs
        ORDER BY pmid
        RETURN pmid, refs
        """
        
        with self.driver.session() as session:
            results = session.run(query)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                count = 0
                for record in results:
                    pmid = record['pmid']
                    refs = record['refs']

                    refs = [str(ref) for ref in refs if ref]
                    
                    if refs:
                        line = pmid + "/" + "/".join(refs) + "\n"
                    else:
                        line = pmid + "\n"
                    
                    f.write(line)
                    count += 1
                    
                    if count % 10000 == 0:
                        print(f"  Exporté {count} documents...")
            
            print(f"✓ {count} documents exportés dans {output_file}")
    
        end_time = time.time()
        elapsed = end_time - start_time

        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)

        print(f"\n Temps total de génération du second fichier : {minutes} min {seconds} sec")

    def get_statistics(self):
        """Afficher des statistiques sur les données"""
        print("\n=== Statistiques ===")
        
        with self.driver.session() as session:
            # Nombre total de documents
            result = session.run("MATCH (d:Document) RETURN count(d) AS count")
            total = result.single()['count']
            print(f"Total de documents: {total}")
            
            # Documents avec abstract
            result = session.run("""
                MATCH (d:Document)
                WHERE d.abstract IS NOT NULL AND d.abstract <> ''
                RETURN count(d) AS count
            """)
            with_abstract = result.single()['count']
            print(f"Documents avec abstract: {with_abstract}")
            
            # Statistiques de références
            result = session.run("""
                MATCH (d:Document)
                OPTIONAL MATCH (d)-[:REFERENCES]->(out:Document)
                WITH count(DISTINCT out) AS ref_count
                RETURN avg(ref_count) AS avg_refs, max(ref_count) AS max_refs
            """)
            stats = result.single()
            print(f"Moyenne de références par document: {stats['avg_refs']:.2f}")
            print(f"Maximum de références: {stats['max_refs']}")

def main():
    
    

    # Paramètres de connexion
    uri = sys.argv[1] if len(sys.argv) > 1 else "bolt://localhost:7687"
    user = sys.argv[2] if len(sys.argv) > 2 else "neo4j"
    password = sys.argv[3] if len(sys.argv) > 3 else "password"
    
    # Créer l'exporteur
    exporter = Neo4jExporter(uri, user, password)
    
    try:
        # Afficher les statistiques
        exporter.get_statistics()
        
        # Exporter les données
        exporter.export_documents_with_text("resultat_Neo4j_3_1.txt")
        exporter.export_documents_with_references("resultat_Neo4j_3_2.txt")
        
        print("\n✓ Export terminé!")
        
    finally:
        exporter.close()
    

if __name__ == '__main__':
    main()