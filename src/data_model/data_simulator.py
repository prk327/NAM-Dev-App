import yaml
from pathlib import Path
from faker import Faker
import random
from datetime import datetime, timedelta
from db_operations import VerticaDB
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Iterable, Tuple

class DataSimulator:

    DEFAULT_PRIMARY_KEY_SCHEMA = {
        "type": "INT",
        "constraints": ["primary_key", "auto_increment"],
        "simulation": {
            "type": "sequence",
            "start": 1000,
            "step": 1
        }
    }

    DEFAULT_COLUMN_SCHEMA = {
            "type": "VARCHAR(255)",
            "constraints": [],
            "simulation": {
                "type": "faker",
                "provider": "word"
            }
        }
        
        
    def __init__(self, config_path):
        self.faker = Faker()
        self.config = self._load_config(config_path)
        self.tables = self._load_table_configs()
        self.columns = self._load_column_configs()
        self.generated_data = {}
        self.db = VerticaDB()
        self.executor = ThreadPoolExecutor(max_workers=self.config.get('max_workers', 4))

    def _load_config(self, config_path):
        with open(config_path) as f:
            return yaml.safe_load(f)

    def _load_table_configs(self):
        tables_dir = Path(__file__).parent / 'config' / 'tables'
        table_configs = {}
        
        for table_file in tables_dir.glob('*.yaml'):
            with open(table_file) as f:
                table_data = yaml.safe_load(f)
                table_name = table_data['table_name']
                table_configs[table_name] = table_data
                
        return table_configs
        
    def _load_column_configs(self):
        columns_dir = Path(__file__).parent / 'config' / 'columns'
        column_configs = {}

        for column_file in columns_dir.glob('*.yaml'):
            with open(column_file) as f:
                column_data = yaml.safe_load(f)
                column_configs.update(column_data.get('columns', {}))

        return column_configs
        
    def get_table_schema(self, table_name):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found")
        
        table_config = self.tables[table_name]
        schema = {}

        # Add primary key column
        if 'primary_key' in table_config:
            pk_column = table_config['primary_key']
            schema[pk_column] = self.DEFAULT_PRIMARY_KEY_SCHEMA

        # Add other columns
        for col in table_config.get('columns', []):
            if col in self.columns:
                schema[col] = self.columns[col]
            else:
                schema[col] = self.DEFAULT_COLUMN_SCHEMA
                print(f"Warning: Using default schema for column {col} in table {table_name}")

        # Add foreign key columns
        for fk in table_config.get('foreign_keys', []):
            fk_column = fk['column']
            ref_table = fk['references']['table']
            ref_column = fk['references']['column']

            # Fetch foreign key definition from referenced table
            if ref_table in self.tables:
                ref_schema = self.get_table_schema(ref_table)
                if ref_column in ref_schema:
                    schema[fk_column] = {
                        "type": ref_schema[ref_column]["type"],  # Use the same type as the referenced column
                        "constraints": [f"foreign_key: {ref_table}.{ref_column}"],
                        "simulation": fk.get('simulation', {})
                    }
                else:
                    # If referenced column not found, use default schema
                    schema[fk_column] = {
                        "type": "INT",  # Default type for foreign keys
                        "constraints": [f"foreign_key: {ref_table}.{ref_column}"],
                        "simulation": fk.get('simulation', {})
                    }
                    print(f"Warning: Referenced column {ref_column} not found in table {ref_table}. Using default schema.")
            else:
                # If referenced table not found, use default schema
                schema[fk_column] = {
                    "type": "INT",  # Default type for foreign keys
                    "constraints": [f"foreign_key: {ref_table}.{ref_column}"],
                    "simulation": fk.get('simulation', {})
                }
                print(f"Warning: Referenced table {ref_table} not found. Using default schema.")
        
        return schema

    def generate_data(self, table_name, num_records):
        schema = self.get_table_schema(table_name)
        records = []
        
        for _ in range(num_records):
            record = {}
            for col, col_config in schema.items():
                if col_config['simulation'].get('type') == 'reference':
                    # db = VerticaDB()
                    # Handle foreign key simulation
                    ref_table = col_config['simulation']['table']
                    ref_column = col_config['simulation']['column']
                    # Need to implement Enrichment table lookup
                    record[col] = random.choice([columnsItems[0] for columnsItems in self.db.read(ref_table, columns=ref_column)])
                    # if ref_table in self.generated_data:
                    #     record[col] = random.choice(self.generated_data[ref_table])[ref_column]
                    # else:
                    #     raise ValueError(f"Referenced table {ref_table} not found in generated data")
                else:
                    # Handle regular column simulation
                    record[col] = self._generate_column_data(col_config)
            records.append(record)
            
        self.generated_data[table_name] = records
        return records

    def generate_data_parallel(self, table_name, num_records, batch_size=1000):
        futures = []
        for i in range(0, num_records, batch_size):
            futures.append(self.executor.submit(self.generate_data, table_name, min(batch_size, num_records - i)))
        results = []
        for future in as_completed(futures):
            results.extend(future.result())
        return results

    def _generate_column_data(self, col_config):
        sim_config = col_config.get('simulation', {})
        
        if sim_config.get('type') == 'sequence':
            return self._handle_sequence(col_config)
            
        elif sim_config.get('type') == 'faker':
            provider = sim_config['provider']
            params = sim_config.get('params', {})
            
            # Handle product_name simulation
            if provider == 'words':
                # Simulate product names using 'word' or 'sentence'
                return ' '.join(self.faker.words(**params))
            else:
                # Use the specified Faker provider
                return getattr(self.faker, provider)(**params)
                
        elif sim_config.get('type') == 'enum':
            return random.choices(
                sim_config['values'],
                weights=sim_config.get('weights'),
                k=1
            )[0]
            
        elif sim_config.get('type') == 'random':
            return self._generate_random_value(sim_config)
            
        elif sim_config.get('type') == 'date':
            return self._generate_date(sim_config)
            
        return None  # Add default handlers as needed

    def _handle_sequence(self, col_config):
        """
        Generate sequential values for columns with 'sequence' simulation type.
        
        Args:
            col_config (dict): Column configuration containing simulation rules.
            
        Returns:
            int: The next value in the sequence.
            
        Raises:
            ValueError: If the simulation configuration is invalid.
        """
        # Initialize sequences dictionary if it doesn't exist
        if not hasattr(self, '_sequences'):
            self._sequences = {}
        
        # Validate simulation configuration
        simulation_config = col_config.get('simulation', {})
        if not simulation_config or simulation_config.get('type') != 'sequence':
            raise ValueError("Invalid simulation configuration for sequence type.")
        
        # Use column name as the sequence key
        seq_name = col_config.get('name', 'default_sequence')
        
        # Initialize sequence if it doesn't exist
        if seq_name not in self._sequences:
            start = simulation_config.get('start', 1)  # Default start value is 1
            if not isinstance(start, int):
                raise ValueError("Sequence 'start' value must be an integer.")
            self._sequences[seq_name] = start
        
        # Get the current value and increment the sequence
        current = self._sequences[seq_name]
        step = simulation_config.get('step', 1)  # Default step value is 1
        if not isinstance(step, int):
            raise ValueError("Sequence 'step' value must be an integer.")
        
        self._sequences[seq_name] += step
        return current

    def _generate_random_value(self, sim_config):
        dist_type = sim_config['distribution']
        params = sim_config['params']
        
        if dist_type == 'normal':
            val = random.gauss(params['mean'], params['std_dev'])
            return max(min(val, params['max']), params['min'])
            
        elif dist_type == 'uniform':
            return random.uniform(params['min'], params['max'])
            
    def _generate_date(self, sim_config):
        start = datetime.strptime(sim_config['range']['start'], '%Y-%m-%d')
        end = datetime.strptime(sim_config['range']['end'], '%Y-%m-%d')
        delta = (end - start).days
        return (start + timedelta(days=random.randint(0, delta))).date()

    def convert_list_of_dicts_to_tuples(self, data: List[Dict]) -> Dict[str, Iterable]:
        # Extract column names from the first dictionary
        columns = list(data[0].keys())
        
        # Extract values from each dictionary and convert them to tuples
        values = [tuple(d.values()) for d in data]
        
        # Return the result in the desired format
        return columns, values

    def yield_data(self, listTuples):
        for i in listTuples:
            yield i
        yield None  # Sentinel value to signal end of data
# Usage Example
if __name__ == "__main__":
    simulator = DataSimulator('config/data_config.yaml')

    products = simulator.generate_data_parallel('products', 500)
    simulator.db.batch_insert('products', products)

    # Generate users
    users = simulator.generate_data_parallel('users', 100)
    simulator.db.batch_insert('users', users)
    # print(users)
    
    # Generate orders referencing users
    orders = simulator.generate_data_parallel('orders', 500)
    
    # Insert into Vertica
    # db = VerticaDB()  # From previous module
    simulator.db.batch_insert('orders', orders)