package main

import (
	"fmt"
	"log"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

func Transform(config MigrationConfig, doc bson.M) DestinationData {
	data := DestinationData{status: false}

	key, keyExists := doc[config.MongoIDField].(string)
	if !keyExists {
		log.Fatalf("Key not found in document: %v", doc)
		return data
	}
	value, valueExists := doc["value"]
	score, scoreExists := doc["score"]
	members, membersExists := doc["members"]
	if !membersExists {
		array, arrayExists := doc["array"]
		members = array
		membersExists = arrayExists
	}

	data.status = true
	data.key = parseString(key)

	// TODO: Handle cluster mode
	if config.RedisCluster && data.useCustomKeySlot {
		// Example: If cluster mode, add curly braces to the first part of the key
		// This is to allow for key sharding in cluster mode
		keyParts := strings.Split(data.key, ":")
		if len(keyParts) > 1 {
			keyParts[0] = fmt.Sprintf("{%s}", keyParts[0])
			data.key = strings.Join(keyParts, ":")
		}
	}

	// If sorted set
	if scoreExists || score != nil {
		data.score = parseNumber(score)
		data.value = parseString(value)
		data.dataType = DataSortedSet
		return data
	}

	// If list
	if membersExists || members != nil {
		data.members = parseArray(members)
		data.dataType = DataTypeList
		return data
	}

	// If hash - no value or has more that just id, _key, value
	if !valueExists || len(doc) > 3 {
		delete(doc, config.MongoIDField)
		if config.MongoExcludeIDField {
			delete(doc, "_id")
		}
		data.hash = parseHash(doc)
		data.dataType = DataTypeHash
		return data
	}

	// If string
	data.value = parseString(value)
	data.dataType = DataTypeString
	return data
}
