import yaml
from pathlib import Path
from faker import Faker
import random
import uuid
from datetime import datetime, timedelta
from db_operations import VerticaDB

class DataSimulator:
    def __init__(self, config_path):
        self.faker = Faker()
        self.config = self._load_config(config_path)
        self.tables = self._load_table_configs()
        self.generated_data = {}
        self._sequences = {}

    def _load_config(self, config_path):
        with open(config_path) as f:
            return yaml.safe_load(f)

    def _load_table_configs(self):
        tables_dir = Path(__file__).parent / 'config' / 'tables'
        table_configs = {}
        
        for table_file in self.config['tables']:
            table_path = tables_dir / f"{table_file}.yaml"
            with open(table_path) as f:
                table_data = yaml.safe_load(f)
                table_name = table_data['table_name']
                table_configs[table_name] = table_data
                
        return table_configs

    def generate_data(self, table_name, num_records):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not defined")
            
        records = []
        for _ in range(num_records):
            record = {}
            for col in self.tables[table_name]['columns']:
                record[col['name']] = self._generate_column_data(col)
            records.append(record)
            
        self.generated_data.setdefault(table_name, []).extend(records)
        return records

    def _generate_column_data(self, col_config):
        sim_config = col_config.get('simulation', {})
        
        if sim_config.get('type') == 'sequence':
            return self._handle_sequence(col_config)
            
        elif sim_config.get('type') == 'faker':
            return getattr(self.faker, sim_config['provider'])(**sim_config.get('params', {}))
            
        elif sim_config.get('type') == 'enum':
            return random.choices(
                sim_config['values'],
                weights=sim_config.get('weights'),
                k=1
            )[0]
            
        elif sim_config.get('type') == 'random':
            return self._generate_random_value(sim_config)
            
        elif sim_config.get('type') == 'reference':
            return random.choice(self.generated_data.get(
                sim_config['table'], 
                [[]]  # Fallback empty list
            ))[sim_config['column']]
            
        elif sim_config.get('type') == 'date':
            return self._generate_date(sim_config)
            
        return None  # Add default handlers as needed

    def _handle_sequence(self, col_config):
        # Implement sequence tracking
        if not hasattr(self, '_sequences'):
            self._sequences = {}
            
        seq_name = col_config['name']
        if seq_name not in self._sequences:
            self._sequences[seq_name] = col_config['simulation']['start']
            
        current = self._sequences[seq_name]
        self._sequences[seq_name] += col_config['simulation'].get('step', 1)
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
# Usage Example
if __name__ == "__main__":
    simulator = DataSimulator('config/data_config.yaml')
    
    # Generate users
    users = simulator.generate_data('users', 100)
    # print(users)
    
    # Generate orders referencing users
    orders = simulator.generate_data('orders', 500)
    
    # Insert into Vertica
    db = VerticaDB()  # From previous module
    db.batch_insert('omniq.users', users)
    db.batch_insert('orders', orders)