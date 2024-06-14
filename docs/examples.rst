Examples
========

Here are some example usages of Nightingale with various configurations and sample data.

Example 1: Basic Transformation
-------------------------------

1. **Sample Configuration File:**

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

2. **Sample Mapping File (`mapping.xlsx`):**

   **General Sheet:**

   .. code-block:: text

       id  | Title          | Description    | Path                        | Status   | Mapping                             | Comment
       ----|----------------|----------------|-----------------------------|----------|------------------------------------ | -------
       1   | OCID           | unique ID      | ocid                        | Required | example_table (id)                 | -
       2   | Party Identifier  | Identifier  | parties/[0]/identifier/id   | Required | party_table (identifier)           | -
       3   | Party Name     | Name           | parties/[0]/name            | Required | party_table (name)                 | -
       4   | Party Role     | Role           | parties/[0]/roles           | Required | party_table (role)                 | -

   **Tender Sheet:**

   .. code-block:: text

       id  | Title          | Description     | Path                       | Status   | Mapping                             | Comment
       ----|----------------|-----------------|----------------------------|----------|------------------------------------ | -------
       1   | Tender Title   | Name of tender  | tender/title               | Optional | example_table (name)               | -
       2   | Value          | Tender value    | tender/value/amount        | Optional | example_table (value)              | -

3. **Sample SQLite Database:**

   See the `create_sample_data.py` script from the tutorial.

4. **Run the Transformation:**

   .. code-block:: sh

       nightingale --config sample_config.toml --loglevel INFO

Example 2: Transformation with Packaging
----------------------------------------

1. **Sample Configuration File:**

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

2. **Sample Mapping File (`mapping.xlsx`):**

   Use the same `mapping.xlsx` as in Example 1.

3. **Run the Transformation with Packaging:**

   .. code-block:: sh

       nightingale --config sample_config.toml --package --loglevel INFO

This command will not only map the data but also package it into a release package and write it to the `output` directory.

Example 3: Advanced SQL Query for Data Manipulation
---------------------------------------------------

You may need to manipulate data within the SQL query itself before it is fed into the mapper. Here’s an example demonstrating advanced SQL usage.

1. **Sample Configuration File:**

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
           party_table.role AS "party_table (role)",
           CASE
               WHEN value IS NOT NULL THEN 'Non-Null Value'
               ELSE 'Null Value'
           END AS "example_table (value_status)"
       FROM example_table
       JOIN party_table ON example_table.id = party_table.id
       WHERE example_table.id > 1
       ORDER BY example_table.name;
       '''

       [publishing]
       publisher = 'Sample Publisher'
       version = '1.1'

       [output]
       directory = 'output'

2. **Sample Mapping File (`mapping.xlsx`):**

   **General Sheet:**

   .. code-block:: text

       id  | Title          | Description    | Path                        | Status   | Mapping                             | Comment
       ----|----------------|----------------|-----------------------------|----------|------------------------------------ | -------
       1   | OCID           | unique ID      | ocid                        | Required | example_table (id)                 | -
       2   | Party Identifier  | Identifier  | parties/[0]/identifier/id   | Required | party_table (identifier)           | -
       3   | Party Name     | Name           | parties/[0]/name            | Required | party_table (name)                 | -
       4   | Party Role     | Role           | parties/[0]/roles           | Required | party_table (role)                 | -

   **Tender Sheet:**

   .. code-block:: text

       id  | Title          | Description     | Path                       | Status   | Mapping                             | Comment
       ----|----------------|-----------------|----------------------------|----------|------------------------------------ | -------
       1   | Tender Title   | Tender title    | tender/title               | Optional | example_table (name)               | -
       2   | Value          | Tender value    | tender/value/amount        | Optional | example_table (value)              | -
       3   | Value Status   | Value status    | tender/status              | Optional | example_table (value_status)       | -

3. **Sample SQLite Database:**

   Use the `create_sample_data.py` script from the tutorial.

4. **Run the Transformation with Advanced SQL:**

   .. code-block:: sh

       nightingale --config sample_config.toml --loglevel INFO

This command will execute the advanced SQL query, manipulate the data, and then transform it using the specified mapping configuration.

Example 4: Joining Data from Multiple Tables
--------------------------------------------

If the required data spans across multiple tables, you can use SQL JOINs to combine the data before mapping.

1. **Create Additional Sample Data:**

   Modify the `create_sample_data.py` script to create and populate additional tables:

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
       CREATE TABLE another_table (
           id INTEGER PRIMARY KEY,
           example_id INTEGER,
           description TEXT,
           FOREIGN KEY (example_id) REFERENCES example_table(id)
       )
       ''')

       # Insert sample data
       cursor.executemany('''
       INSERT INTO example_table (name, value) VALUES (?, ?)
       ''', [
           ('sample1', 'value1'),
           ('sample2', 'value2'),
           ('sample3', 'value3'),
       ])

       cursor.executemany('''
       INSERT INTO another_table (example_id, description) VALUES (?, ?)
       ''', [
           (1, 'Description 1'),
           (2, 'Description 2'),
           (3, 'Description 3'),
       ])

       # Commit and close
       conn.commit()
       conn.close()

2. **Sample Configuration File:**

   .. code-block:: toml

       [datasource]
       connection = 'sample_database.db'

       [mapping]
       file = 'mapping.xlsx'
       ocid_prefix = 'ocds-123abc'
       force_publish = true
       selector = '''
       SELECT
           e.id AS "example_table (id)",
           e.name AS "example_table (name)",
           e.value AS "example_table (value)",
           a.description AS "another_table (description)"
       FROM example_table e
       JOIN another_table a ON e.id = a.example_id;
       '''

       [publishing]
       publisher = 'Sample Publisher'
       version = '1.1'

       [output]
       directory = 'output'

3. **Sample Mapping File (`mapping.xlsx`):**

   **General Sheet:**

   .. code-block:: text

       id  | Title          | Description    | Path                        | Status   | Mapping                             | Comment
       ----|----------------|----------------|-----------------------------|----------|------------------------------------ | -------
       1   | OCID           | unique ID      | ocid                        | Required | example_table (id)                 | -
       2   | Party Identifier  | Identifier  | parties/[0]/identifier/id   | Required | party_table (identifier)           | -
       3   | Party Name     | Name           | parties/[0]/name            | Required | party_table (name)                 | -
       4   | Party Role     | Role           | parties/[0]/roles           | Required | party_table (role)                 | -

   **Tender Sheet:**

   .. code-block:: text

       id  | Title          | Description  | Path                       | Status   | Mapping                             | Comment
       ----|----------------|--------------|----------------------------|----------|------------------------------------ | -------
       1   | Tender Title   | Tender title | tender/title               | Optional | example_table (name)               | -
       2   | Value          | Tender value | tender/value/amount        | Optional | example_table (value)              | -

   **Contract Sheet:**

   .. code-block:: text

       id  | Title          | Description                    | Path                           | Status   | Mapping                            | Comment
       ----|----------------|--------------------------------|-------------------------------|----------|----------------------------------- | -------
       1   | Description    | Description from another table | contracts/[0]/description     | Optional  | another_table (description)       | -

4. **Run the Transformation with SQL JOIN:**

   .. code-block:: sh

       nightingale --config sample_config.toml --loglevel INFO

This command will execute the SQL query joining data from two tables and then transform it using the specified mapping configuration.
