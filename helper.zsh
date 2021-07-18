#!/usr/bin/env zsh

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

export host_development_dir="$HOME/development/big-data-gcw"

function connect_to_root() {
  ssh root@localhost -p 2222
}

function move_required_files() {
  scp -P 2222 "$host_development_dir"/clean.data \
    "$host_development_dir"/helper.zsh \
    "$host_development_dir"/FindBrowserPercentage.py \
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
    hdfs dfs -test -e "$data_dir/clean.data" &&
    if $? == 0; then
      hdfs_remove "$data_dir/clean.data" # First remove the old data file.
    fi &&
    hdfs_put ~/clean.data $data_dir &&
    hdfs dfs -chmod 777 "$data_dir/clean.data" # Access to all

}
