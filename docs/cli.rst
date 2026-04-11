CLI Reference
=============

.. code-block:: bash

    ocdsnightingale --help

--config <path>
    Path to the configuration file. This option is required.

--package
    Package the data into a release package.

--stream / --no-stream
    Enable or disable streaming to the output file. Streaming is enabled by default.

--validate-mapping
    Validate the mapping template against the data source.

--loglevel <level>
    Set the logging level. Available levels are: DEBUG, INFO, WARNING, ERROR, CRITICAL.

--datasource <connection>
    Datasource connection string. Overrides the ``[datasource] connection`` configuration.

--mapping-file <path>
    Path to the mapping file. Overrides the ``[mapping] file`` configuration.

--codelists-file <path>
    Path to the codelists mapping file. Overrides the ``[mapping] codelists`` configuration.

--ocid-prefix <prefix>
    OCID prefix. Overrides the ``[mapping] ocid_prefix`` configuration.

--selector <path>
    Path to a SQL script file containing the selector query. Overrides the ``[mapping] selector`` configuration.

--force-publish
    Force publish all mapped fields. Overrides the ``[mapping] force_publish`` configuration.

--publisher <name>
    Publisher name. Overrides the ``[publishing] publisher`` configuration.

--base-uri <uri>
    Package base URI. Overrides the ``[publishing] base_uri`` configuration.

--version <version>
    OCDS version. Overrides the ``[publishing] version`` configuration.

--publisher-uid <uid>
    Publisher UID. Overrides the ``[publishing] publisher_uid`` configuration.

--publisher-scheme <scheme>
    Publisher scheme. Overrides the ``[publishing] publisher_scheme`` configuration.

--publisher-uri <uri>
    Publisher URI. Overrides the ``[publishing] publisher_uri`` configuration.

--extensions <url>
    Extension URL. Can be specified multiple times. Overrides the ``[publishing] extensions`` configuration.

--output-directory <path>
    Output directory. Overrides the ``[output] directory`` configuration.
