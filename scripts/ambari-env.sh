#!/usr/bin/env bash

# ----------------------------------------------------------------
# HOST environment commands
# ----------------------------------------------------------------

# Yasin's Credentials
#
# root: Yas!n123
# admin: admin
# maria_dev: maria_dev

# To move the processed dataset to root home:
#
# scp -P 2222 ~/bda-cw-workdir/clean.data root@localhost:~/.
# scp -P 2222 ~/development/big-data-gcw/helper.zsh root@localhost:~/.
# scp -P 2222 ~/development/big-data-gcw/helper.zsh maria_dev@localhost:~/.

# TODO: Change to your local directory.
export host_development_dir="$HOME/development/big-data-gcw"
export data_file_name="access-logs.data"

function connect_to_root() {
  ssh root@localhost -p 2222
}

function move_required_files() {
  scp -P 2222 \
    "$host_development_dir/resources/$data_file_name" \
    "$host_development_dir/scripts/ambari-env.zsh" \
    "$host_development_dir/scripts/RDDQueries.py" \
    root@localhost:~/.
}

# ----------------------------------------------------------------
# HDP environment commands
# ----------------------------------------------------------------

alias hdfs_put="hdfs dfs -put"
alias hdfs_remove="hdfs dfs -rm"

function move_data_file_to_maria_dev() {

  local data_dir="/user/maria_dev/access-logs"

  echo "You should execute this inside ~ home" &&
    # If only exists
    hdfs dfs -test -e "$data_dir/$data_file_name" &&
    if $? == 0; then
      hdfs_remove "$data_dir/$data_file_name" # First remove the old data file.
    fi &&
    hdfs_put "$HOME/$data_file_name" "$data_dir" &&
    hdfs dfs -chmod 777 "$data_dir/$data_file_name" # Access to all

}
