# big-data-gcw

Big Data &amp; Analytics Group Coursework

# Getting started (Goals)

1. Download the [dataset](https://www.kaggle.com/eliasdabbas/web-server-access-logs?select=access.log)
2. Normalize the dataset using `normalize.py`
3. Load it to ambari/hive-view and play with it.

# Normalize the dataset

1. Install python `3.5` or above
2. Execute it! `python3 normalize.py /Users/yasin/Downloads/access.log 20000`
   > Optionally to create a smaller dataset `python3 normalize.py <path> <batch_size> <dry_run_limit>`
3. All the cleaned data will be in `$HOME/bda-cw-workdir`

# Appendix

1. [How to move large files via terminal](https://www.cloudera.com/tutorials/manage-files-on-hdfs-via-cli-ambari-files-view/1.html)
2. [How to reset Ambari Admin password](https://community.cloudera.com/t5/Community-Articles/Ambari-2-7-0-How-to-Reset-Ambari-Admin-Password-from/ta-p/248891)