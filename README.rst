===========
agrometflow
===========

.. image:: https://mybinder.org/badge_logo.svg
   :target: https://mybinder.org/v2/gh/cyrillemidingoyi/agrometflow/HEAD?labpath=notebooks%2Fpipeline.ipynb
   :alt: Launch Binder

A Python package for automated download and processing of climate and soil data
for soil-plant-atmosphere modeling workflows.

Features
--------

- Unified interface for multiple climate data sources (POWER, ERA5, etc.)
- Modular structure for easy integration of new sources
- Automated pipeline for preprocessing (extraction, resampling, etc.)

Training Material
-----------------

- GitHub Pages support: ``docs/index.html``
- Training notebooks: ``notebooks/formation_cameroon/``
- Binder classroom entrypoint:

.. code-block:: text

   https://mybinder.org/v2/gh/cyrillemidingoyi/agrometflow/HEAD?urlpath=lab/tree/notebooks/formation_cameroon

Install
-------

.. code-block:: bash

   pip install agrometflow

Run Without Notebook
--------------------

For training sessions, participants can run downloads from a YAML file without
writing Python:

.. code-block:: bash

   agrometflow-run examples/lsasaf_etp_points.yml --max-workers 2

In Binder, keep ``--max-workers`` between ``1`` and ``2`` to avoid exhausting the
session memory.
