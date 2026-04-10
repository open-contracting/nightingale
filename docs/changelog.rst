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

-  Keyword arguments are required for boolean arguments: :commit:`4268fcc`

   -  :meth:`nightingale.mapper.OCDSDataMapper.map`
   -  :meth:`nightingale.mapping_template.v09.MappingTemplate.get_paths_for_mapping`

-  Rename package and command to ``ocdsnightingale``.

0.0.1 (2026-10-04)
------------------

First release.
