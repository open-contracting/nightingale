Changelog
=========

Unreleased
----------

Fixed
~~~~~

-  Restore release date assignment. :commit:`c1d585f`
-  The ``--version`` option no longer controls ``license`` and ``publicationPolicy``. :commit:`c6953ae`
-  The ``force_publish`` configuration is no longer ignored. :commit:`c6953ae`

Changed
~~~~~~~

-  Datetimes use UTC with a ``Z`` suffix (``2026-04-10T21:30:00Z``) instead of US Pacific with an offset (``2026-04-10T14:30:00-07:00``). This affects:

   - the release's ``date``, when not derivable from data
   - the package's ``publishedDate``
   - the package's ``uri``
   - the package's filename

-  Keyword arguments are required for boolean arguments: :commit:`4268fcc`

   -  :meth:`nightingale.mapper.OCDSDataMapper.map`
   -  :meth:`nightingale.mapping_template.v09.MappingTemplate.get_paths_for_mapping`

-  Rename package and command to ``ocdsnightingale``.

0.0.1 (2026-10-04)
------------------

First release.
