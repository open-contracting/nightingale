Changelog
=========

0.0.2 (2026-04-11)
------------------

Fixed
~~~~~

-  Restore release date assignment. :commit:`c1d585f`
-  The ``--version`` option no longer controls ``license`` and ``publicationPolicy``. :commit:`c6953ae`
-  The ``force_publish`` configuration is no longer ignored. :commit:`c6953ae`
-  Remove hardcoding of OCID prefix in logger messages.

Changed
~~~~~~~

Dates
^^^^^

-  Datetimes use UTC with a ``Z`` suffix (``2026-04-10T21:30:00Z``) instead of US Pacific with an offset (``2026-04-10T14:30:00-07:00``). This affects:

   - the release's ``date``, when not derivable from data
   - the package's ``publishedDate``
   - the package's ``uri``
   - the package's filename

-  If ``--no-stream`` is set and ``--package`` isn't set, then the package's filename defaults to e.g. ``2026-04-10T21:30:00Z`` (``Z`` suffix and no microseconds), instead of e.g. ``2026-04-10T14:30:00.123456`` (microseconds and no ``Z`` suffix).
-  :meth:`nightingale.writer.DataWriter.write` and :func:`nightingale.writer.new_name` require the ``package`` argument to set ``"publishedDate"``, when the package is a ``dict``.
-  :meth:`nightingale.writer.DataWriter.start_package_stream` requires the ``package_metadata`` argument to set ``"publishedDate"``.

Codes
^^^^^

-  Unmapped codelist values are no longer passed through (instead of discarded) for ``/tender/status`` and ``/tender/procurementMethod`` by default. To migrate:

   .. code-block:: toml

      [mapping]
      codelist_passthrough_paths = ["/tender/status", "/tender/procurementMethod"]

-  The milestone lookup table (``[releases to Dte]``) is no longer queried by default. To migrate:

   .. code-block:: toml

      [mapping]
      milestone_lookup_sql = "SELECT code, title, description FROM [releases to Dte]"

- The splitting of space-separated milestone codes is no longer performed by default. To migrate:

   .. code-block:: toml

      [mapping]
      split_milestone_codes = true

-  Rename :class:`nightingale.mapper.OCDSDataMapper`'s milestone lookup attribute from ``release_lookup`` to ``milestone_lookup``.

API changes
^^^^^^^^^^^

-  Keyword arguments are required for boolean arguments: :commit:`4268fcc`

   -  :meth:`nightingale.mapper.OCDSDataMapper.map`
   -  :meth:`nightingale.mapping_template.v09.MappingTemplate.get_paths_for_mapping`

-  Rename package to ``ocdsnightingale``.
-  Rename command to ``ocdsnightingale``.
-  Rename ``ocdsnightingale.utils`` module to :mod:`ocdsnightingale.util`.
-  Rename ``ocdsnightingale.cli`` module to :mod:`ocdsnightingale.__main__`.
-  Unpin dependencies.

Removed
~~~~~~~

-  Move Portland configuration files to `open-contracting/data-support <https://github.com/open-contracting/data-support/tree/main/portland_ocds>`__.

0.0.1 (2026-04-10)
------------------

First release.
