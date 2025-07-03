import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import logging
import argparse
from datetime import datetime, date

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CassandraDateScanner:
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

    # def create_keyspace_and_table(self):
    #     """Create keyspace and table if they don't exist"""
    #     try:
    #         # Create keyspace
    #         self.session.execute(f"""
    #             CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
    #             WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    #         """)
            
    #         # Use keyspace
    #         self.session.set_keyspace(self.keyspace)
            
    #         # Create table
    #         self.session.execute("""
    #             CREATE TABLE IF NOT EXISTS generated_data (
    #                 id int PRIMARY KEY,
    #                 string_value text,
    #                 date_value date
    #             )
    #         """)
    #         logger.info("Keyspace and table created/verified")
    #     except Exception as e:
    #         logger.error(f"Failed to create keyspace or table: {str(e)}")
    #         raise

    def scan_table_for_dates(self, table_name='bnp.entite_juridique'):
        """Scan the entire table and find rows where date_revue_du_levier starts with 0001 but is not 0001-01-01"""
        try:
            # Set keyspace
            self.session.set_keyspace(self.keyspace)
            
            # Query to get all rows with date_revue_du_levier
            query = f"""
                SELECT id_rp, code_acteur_impact_esg, date_revue_du_levier 
                FROM {table_name} 
            """
            
            logger.info(f"Scanning table {table_name} for date_revue_du_levier values starting with 0001...")
            
            # Execute query with pagination to handle large datasets
            statement = SimpleStatement(query, fetch_size=1000)
            rows = self.session.execute(statement)
            
            # Filter rows where date_revue_du_levier starts with 0001 but is not 0001-01-01
            target_date_obj = datetime.strptime('0001-01-01', '%Y-%m-%d').date()
            matching_rows = []
            
            for row in rows:
                if row.date_revue_du_levier:
                    # Convert Cassandra date to string using str() method
                    date_str = str(row.date_revue_du_levier)
                    # Check if date starts with 0001 but is not exactly 0001-01-01
                    if date_str.startswith('0001-') and row.date_revue_du_levier != target_date_obj:
                        matching_rows.append({
                            'id_rp': row.id_rp,
                            'code_acteur_impact_esg': row.code_acteur_impact_esg,
                            'date_revue_du_levier': row.date_revue_du_levier,
                            'date_string': date_str
                        })
            
            logger.info(f"Found {len(matching_rows)} rows where date_revue_du_levier starts with 0001 but is not 0001-01-01")
            
            # Display results
            if matching_rows:
                print(f"\nRows where date_revue_du_levier starts with 0001 but is not 0001-01-01:")
                print("-" * 80)
                for i, row in enumerate(matching_rows, 1):
                    print(f"Row {i}:")
                    print(f"  id_rp: {row['id_rp']}")
                    print(f"  code_acteur_impact_esg: {row['code_acteur_impact_esg']}")
                    print(f"  date_revue_du_levier: {row['date_revue_du_levier']} ({row['date_string']})")
                    print()
            else:
                print("No rows found where date_revue_du_levier starts with 0001 but is not 0001-01-01")
            
            return matching_rows
            
        except Exception as e:
            logger.error(f"Failed to scan table: {str(e)}")
            raise

    def close(self):
        """Close the Cassandra connection"""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Scan Cassandra table for dates starting with 0001 but not 0001-01-01')
    parser.add_argument('--table', default='bnp.entite_juridique', help='Table name to scan (default: bnp.entite_juridique)')
    parser.add_argument('--host', default='10.166.72.184', help='Cassandra host (default: 10.166.72.184)')
    parser.add_argument('--port', type=int, default=9042, help='Cassandra port (default: 9042)')
    parser.add_argument('--keyspace', default='geo', help='Cassandra keyspace (default: geo)')
    
    args = parser.parse_args()
    
    # Initialize scanner
    scanner = CassandraDateScanner(host=args.host, port=args.port, keyspace=args.keyspace)
    
    try:
        # Connect to Cassandra
        # If you need authentication, uncomment and modify the following line:
        # scanner.connect(username='your_username', password='your_password')
        scanner.connect()
        
        # Create keyspace and table (if needed)
        # scanner.create_keyspace_and_table()
        
        # Scan table for dates
        scanner.scan_table_for_dates(args.table)
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        scanner.close()

if __name__ == "__main__":
    main() 