Use ddf_utils for ETL tasks
===========================


compare 2 datasets
------------------

``ddf diff`` command compares 2 datasets and return useful statistics
for each indicator.

.. code-block:: bash

   ddf diff -i indicator1 -i indicator2 dataset1 dataset2

For now this command supports following statistics:

- ``rval``: the standard `correlation coefficient`_
- ``avg_pct_chg``: average percentage changes
- ``max_pct_chg``: the maximum of change in percentage
- ``rmse``: the `root mean squared error`_
- ``nrmse``: equals ``rmse``/(max - min) where max and min are
  calculated with data in dataset2
- ``new_datapoints``: datapoints in dataset1 but not dataset2
- ``dropped_datapoints``: datapoints in dataset2 but not dataset1

If no indicator specified in the command, ``rmse`` and ``nrmse`` will
be calculated.

You can also compare 2 commits for a git folder too. In this case you
should run

.. code-block:: bash

   cd dataset_path

   ddf diff --git -o path/to/export/to -i indicator head_ref base_ref

Because the script needs to export different commits for the git repo,
you should provide the ``-o`` flag to set which path you'd like to put
the exported datasets into.

.. _correlation coefficient: https://en.wikipedia.org/wiki/Pearson_correlation_coefficient
.. _root mean squared error: https://medium.com/human-in-a-machine-world/mae-and-rmse-which-metric-is-better-e60ac3bde13d


create DDF dataset from CSV file
--------------------------------

When you have clean CSV data file, you can use the ``ddf from_csv``
command to create DDF dataset.

.. code-block:: bash

   ddf from_csv -i input_file_or_path -o out_path

Where ``-i`` sets the input file or path and when it is a path all
files in the path will be proceed; ``-o`` sets the path the generated
DDF dataset will be put to. If ``-i`` is not set, it defaults to
current path.
