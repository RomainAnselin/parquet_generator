import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import logging
import argparse

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ParquetToCassandra:
    def __init__(self, host='10.166.72.184', port=9042, keyspace='geo'):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.cluster = None
        self.session = None

    def connect(self, username=None, password=None):
        """Connect to Cassandra cluster"""
        try:
            if username and password:
                auth_provider = PlainTextAuthProvider(username=username, password=password)
                self.cluster = Cluster([self.host], port=self.port, auth_provider=auth_provider)
            else:
                self.cluster = Cluster([self.host], port=self.port)
            
            self.session = self.cluster.connect()
            logger.info("Connected to Cassandra cluster")
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {str(e)}")
            raise

    def create_keyspace_and_table(self):
        """Create keyspace and table if they don't exist"""
        try:
            # Create keyspace
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """)
            
            # Use keyspace
            self.session.set_keyspace(self.keyspace)
            
            # Create table
            self.session.execute("""
                CREATE TABLE IF NOT EXISTS generated_data (
                    id_rp int PRIMARY KEY,
                    code_acteur_impact_esg text,
                    date_revue_du_levier date
                )
            """)
            logger.info("Keyspace and table created/verified")
        except Exception as e:
            logger.error(f"Failed to create keyspace or table: {str(e)}")
            raise

    def import_data(self, parquet_file, batch_size):
        """Import data from Parquet file to Cassandra"""
        try:
            # Read Parquet file
            logger.info("Reading Parquet file...")
            df = pd.read_parquet(parquet_file)
            
            # Prepare insert statement
            insert_statement = self.session.prepare("""
                INSERT INTO geo.generated_data (id_rp, code_acteur_impact_esg, date_revue_du_levier)
                VALUES (?, ?, ?)
            """)
            
            # Process in batches
            total_rows = len(df)
            for i in range(0, total_rows, batch_size):
                batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
                end_idx = min(i + batch_size, total_rows)
                batch_df = df.iloc[i:end_idx]
                
                for _, row in batch_df.iterrows():
                    batch.add(insert_statement, (
                        int(row['id_rp']),
                        str(row['code_acteur_impact_esg']),
                        row['date_revue_du_levier']
                    ))
                
                self.session.execute(batch)
                logger.info(f"Processed {end_idx}/{total_rows} rows")
            
            logger.info("Data import completed successfully")
        except Exception as e:
            logger.error(f"Failed to import data: {str(e)}")
            raise

    def close(self):
        """Close the Cassandra connection"""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Import Parquet data to Cassandra')
    parser.add_argument('parquet_file', help='Path to the Parquet file to import')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing (default: 1000)')
    parser.add_argument('--host', default='10.166.72.184', help='Cassandra host (default: 10.166.72.184)')
    parser.add_argument('--port', type=int, default=9042, help='Cassandra port (default: 9042)')
    parser.add_argument('--keyspace', default='bnp', help='Cassandra keyspace (default: geo)')

    args = parser.parse_args()

    # Initialize importer
    importer = ParquetToCassandra(host=args.host, port=args.port, keyspace=args.keyspace)
    
    try:
        # Connect to Cassandra
        # If you need authentication, uncomment and modify the following line:
        # importer.connect(username='your_username', password='your_password')
        importer.connect()
        
        # Create keyspace and table
        # importer.create_keyspace_and_table()
        
        # Import data
        importer.import_data(args.parquet_file, args.batch_size)
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        importer.close()

if __name__ == "__main__":
    main() 
