import re
import yaml

file_path = "/home/jovyan/notebooks/data-platform-00/conf/base/parameters_landing_01.yml"
new_select_table = ["customer","employee"]

def update_select_table(file_path, new_select_table):
    # Read the file content
    with open(file_path, 'r') as file:
        content = file.read()

    # Convert the list of new select_table entries to a string
    select_table_str = ', '.join([f"'{table}'" for table in new_select_table])

    # Use regular expressions to replace the select_table line with the new content
    content = re.sub(r'select_table: \[.*\]', f'select_table: [{select_table_str}]', content)

    # Write the updated content back to the file
    with open(file_path, 'w') as file:
        file.write(content)
    
    print("Updated select table in parameter success!")


# Update the select_table in the YAML file
update_select_table(file_path, new_select_table)