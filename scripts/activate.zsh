#!/usr/bin/env zsh

# Pre-requisite
if [[ -z "$KAFKA_HOME" ]]; then
  echo "You need to add \$KAFKA_HOME to your environment."
  exit 9
fi

# By default zookeeper is running on 2181 and kafka on 9092
# if you have changed the kafka port then make sure to map
# here as the same like in {server,zookeeper}.properties
readonly KAFKA_BIN="$KAFKA_HOME/bin"
readonly ZOOKEEPER_PORT=2181
readonly KAFKA_PORT=9092

function _zookeeper() {
  # Start the ZooKeeper service
  # Note: Soon, ZooKeeper will no longer be required by Apache Kafka.
  # Note: We are using local zookeeper.properties file.
  npx ttab -q -t "Zookeeper" \
    "$KAFKA_BIN/zookeeper-server-start.sh" "configs/zookeeper.properties"
}

function _kafka() {
  # Start the Kafka broker service in a new tab.
  npx ttab -q -t "Kafka Broker" \
    "$KAFKA_BIN/kafka-server-start.sh $KAFKA_HOME/config/server.properties --override log.dirs=kafka-logs/server"
}

function start_zookeeper_and_kafka() {
  echo "Run this in big-data-gcw/ directory." &&
    if ask "Do you want to proceed $USER?" Y; then
      clean_environment &&
        _zookeeper && sleep 5 && _kafka
    fi
}

function clean_environment() {
  if ask "Did you stop all the 'producers' and 'consumers'?" Y; then
    npx kill-port $ZOOKEEPER_PORT $KAFKA_PORT &&
      rm -rf kafka-logs/ output/ checkpoints*/ spark-warehouse/
  else
    echo "Stop them and run me again."
  fi
}

function create_kafka_topic() {
  local topic_name=$1
  if [ -z "$topic_name" ]; then
    echo "You need to give a topic name!" && exit 0
  fi
  "$KAFKA_BIN/kafka-topics.sh" --create \
    --topic "$topic_name" \
    --bootstrap-server localhost:"$KAFKA_PORT"
}

function listen_to_a_topic() {
  "$KAFKA_BIN/kafka-console-consumer.sh" --topic "$1" \
    --from-beginning --bootstrap-server \
    localhost:"$KAFKA_PORT"
}

# Quicker

function create_topics() {
  create_kafka_topic "access-logs" &&
    create_kafka_topic "access-logs-sink"
}

function start_consuming() {
  npx ttab -q -t "access-logs-consumer" \
    "source scripts/activate.zsh && source venv/bin/activate && listen_to_a_topic access-logs"
  npx ttab -q -t "access-logs-sink-consumer" \
    "source scripts/activate.zsh && source venv/bin/activate && listen_to_a_topic access-logs-sink"
}

function ask() {
  local prompt default reply
  if [[ ${2:-} == 'Y' ]]; then
    prompt='Y/n'
    default='Y'
  elif [[ ${2:-} == 'N' ]]; then
    prompt='y/N'
    default='N'
  else
    prompt='y/n'
    default=''
  fi
  while true; do
    # Ask the question (not using "read -p" as it uses stderr not stdout)
    echo -n "$1 [$prompt] "
    # Read the answer (use /dev/tty in case stdin is redirected from somewhere else)
    read -r reply </dev/tty
    # Default?
    if [[ -z $reply ]]; then
      reply=$default
    fi
    # Check if the reply is valid
    case "$reply" in
    Y* | y*) return 0 ;;
    N* | n*) return 1 ;;
    esac
  done
}
