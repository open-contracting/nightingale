Data Flow
=========

Nightingale transforms data through the following steps:

1. Load flat data from the SQLite database using the `selector` from the configuration.
2. Read the mapping configuration to understand how to transform the data.
3. Map the data into the OCDS format.
4. Package the data if specified.
5. Write the data to the specified output.
