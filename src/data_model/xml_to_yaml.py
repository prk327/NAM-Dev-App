import yaml
from pathlib import Path
import xml.etree.ElementTree as ET

def generate_configs(config_path):
    script_dir = Path(__file__).parent
    config = yaml.safe_load((script_dir / config_path).read_text())

    # Resolve paths
    xml_paths = {
        key: (script_dir / value).resolve()
        for key, value in config['xml_path'].items()
    }
    yaml_paths = {
        key: (script_dir / value).resolve()
        for key, value in config['yaml_path'].items()
    }
    tables_to_process = config['tables']

    # Process CDR template XML
    cdr_template_path = xml_paths['cdr_template']
    if not cdr_template_path.exists():
        raise FileNotFoundError(f"CDR template XML not found: {cdr_template_path}")

    tree = ET.parse(cdr_template_path)
    root = tree.getroot()

    # Extract all tables (assuming each table is a direct child of the root)
    all_tables = root.findall('.//cdr')  # Adjust XPath if tables are nested differently

    # Process each table in the XML that matches the config's table list
    for table_element in all_tables:
        table_name = table_element.attrib.get('table-name', '')
        if table_name not in tables_to_process:
            continue  # Skip tables not in the config list

        # Extract primary key and columns
        primary_key = table_element.attrib.get('unique-field', '').replace(' ', '_')
        columns = [
            field.attrib['name'].replace(' ', '_')
            for field in table_element.findall('.//{*}field')
            if field.attrib.get('name')
        ]

        # Build YAML config
        yaml_config = {
            'table_name': table_name,
            'primary_key': primary_key,
            'columns': columns
        }

        # Write to YAML file
        output_path = yaml_paths['tables'] / f"{table_name}.yaml"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            yaml.dump(yaml_config, f, default_flow_style=False)

if __name__ == "__main__":
    generate_configs("config/config.yaml")