Tutorial
========

This tutorial will guide you through the basic usage of Nightingale, transforming flat data from an SQLite database into the OCDS format.

Setup
-----

1. **Prepare a Sample SQLite Database:**

   Create a SQLite database and populate it with some sample data. Here’s an example script (`create_sample_data.py`) to create a sample database:

   .. code-block:: python

       import sqlite3

       # Connect to the database (or create it if it doesn't exist)
       conn = sqlite3.connect('sample_database.db')
       cursor = conn.cursor()

       # Create tables
       cursor.execute('''
       CREATE TABLE example_table (
           id INTEGER PRIMARY KEY,
           name TEXT,
           value TEXT
       )
       ''')

       cursor.execute('''
       CREATE TABLE party_table (
           id INTEGER PRIMARY KEY,
           name TEXT,
           identifier TEXT,
           role TEXT
       )
       ''')

       # Insert sample data into example_table
       cursor.executemany('''
       INSERT INTO example_table (name, value) VALUES (?, ?)
       ''', [
           ('sample1', 'value1'),
           ('sample2', 'value2'),
           ('sample3', 'value3'),
       ])

       # Insert sample data into party_table
       cursor.executemany('''
       INSERT INTO party_table (name, identifier, role) VALUES (?, ?, ?)
       ''', [
           ('party1', 'id1', 'buyer'),
           ('party2', 'id2', 'supplier'),
           ('party3', 'id3', 'procuringEntity'),
       ])

       # Commit and close
       conn.commit()
       conn.close()

   Run the script to create `sample_database.db`.

2. **Create a Sample Configuration File:**

   Create a `sample_config.toml` file with the following content:

   .. code-block:: toml

       [datasource]
       connection = 'sample_database.db'

       [mapping]
       file = 'mapping.xlsx'
       ocid_prefix = 'ocds-123abc'
       force_publish = true
       selector = '''
       SELECT
           example_table.id AS "example_table (id)",
           example_table.name AS "example_table (name)",
           example_table.value AS "example_table (value)",
           party_table.name AS "party_table (name)",
           party_table.identifier AS "party_table (identifier)",
           party_table.role AS "party_table (role)"
       FROM example_table
       JOIN party_table ON example_table.id = party_table.id;
       '''

       [publishing]
       publisher = 'Sample Publisher'
       version = '1.1'

       [output]
       directory = 'output'

3. **Prepare the Mapping File:**

   Use the following configuration for `mapping.xlsx` based on the `ocds field level mapping` template:

   **General Sheet:**

   .. code-block:: text

       | Title         | Description   | Path             | Status   | Mapping                             | Comment
       |---------------|---------------|------------------|----------|------------------------------------ | -------
       | OCID          | unique ID     | ocid             | Required | example_table (id)                 | -
       | Party ID      | Party ID      | parties/id       | Optional | party_table (identifier)           | -
       | Party Name    | Party Name    | parties/name     | Optional | party_table (name)
       | Role          | Role          | parties/roles    | Required | party_table (role)                 | -


   **Tender Sheet:**

   .. code-block:: text

       id  | Title          | Description  | Path              | Status   | Mapping                             | Comment
       ----|----------------|--------------|-------------------|----------|------------------------------------ | -------
       1   | Tender Title   | Tender title | tender/title      | Optional | example_table (name)               | -
       2   | Value          | Tender value | tender/value/amount | Optional | example_table (value)             | -


Running the Transformation
--------------------------

Run the transformation using the CLI:

.. code-block:: sh

    nightingale --config sample_config.toml --package --loglevel DEBUG

This will produce an output file in the `output` directory.

Mapping Configuration
----------------------

Field-level mapping is specified in the `mapping.xlsx` file. It is formed from stardard `"OCDS Field Level Mapping template" <https://www.open-contracting.org/resources/ocds-field-level-mapping-template/>`_.
For more information about how to fill the mapping file, refer to the `OCDS Field Level Mapping template guidance <https://www.open-contracting.org/resources/ocds-1-1-mapping-template-guidance/>`_.

Here the bried description of the columns from mapping sheets in the mapping file:

    * **Path**: The path in the OCDS release schema where the field value should be placed.
    * **Title**: A human-readable title for the field.
    * **Description**: A description of what the field represents.
    * **Mapping**: The field in the source data that maps to the OCDS path.

Understanding these mappings will help you configure the transformation correctly for your data.
