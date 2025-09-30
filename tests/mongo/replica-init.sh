#!/bin/bash

# Configuration
HOSTS=(${MONGO_HOST_1} ${MONGO_HOST_2}) # TODO: Add hosts here (space separated)

PORT=${MONGO_PORT}
USER=${MONGO_INITDB_ROOT_USERNAME}
PASS=${MONGO_INITDB_ROOT_PASSWORD}
TIMEOUT=60
RS_NAME="${MONGO_RS}"

echo "###### Waiting for all MongoDB instances to be ready..."
for host in "${HOSTS[@]}"; do
  until mongosh --host "$host" --quiet --eval "db.runCommand({ ping: 1 }).ok" &>/dev/null; do
    echo -n "."
    sleep 1
  done
  echo " $host ready"
done

echo "###### Checking replica set status..."
if mongosh --host "${HOSTS[0]}" --username "$USER" --password "$PASS" --authenticationDatabase admin \
   --quiet --eval "rs.status().ok" &>/dev/null; then
  echo "###### Replica set already initialized"
  exit 0
fi

echo "###### Initializing replica set..." 
# TODO: Add hosts here
CONFIG=$(cat <<EOF
{
  "_id": "$RS_NAME",
  "members": [
    { "_id": 0, "host": "${HOSTS[0]}:$PORT", "priority": 2 },
    { "_id": 1, "host": "${HOSTS[1]}:$PORT", "priority": 1 },
  ]
}
EOF
)

# Initialize replica set
mongosh --host ${HOSTS[0]} --username $USER --password $PASS --authenticationDatabase admin <<EOF
// Initialize or reconfigure replica set
try {
  rs.initiate($CONFIG);
} catch (e) {
  if (e.codeName === 'AlreadyInitialized') {
    print("Reconfiguring existing replica set...");
    rs.reconfig($CONFIG, {force: true});
  } else {
    throw e;
  }
}

// Wait for primary election
print("Waiting for primary to be elected...");
var timeout = $TIMEOUT * 1000;
var start = Date.now();
while (!db.hello().isWritablePrimary && (Date.now() - start) < timeout) {
  sleep(1000);
}

if (!db.hello().isWritablePrimary) {
  print("Error: Primary not elected within timeout");
  quit(1);
}

print("Replica set initialized successfully:");
printjson(rs.status());
EOF

exit $?