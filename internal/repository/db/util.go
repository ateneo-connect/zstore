package db

import (
	"encoding/base64"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func encodeStartKey(key map[string]types.AttributeValue) (string, error) {
	if key == nil {
		return "", nil
	}
	data, err := json.Marshal(key)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

func decodeStartKey(token string) (map[string]types.AttributeValue, error) {
	if token == "" {
		return nil, nil
	}
	data, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil, err
	}
	var key map[string]types.AttributeValue
	if err := json.Unmarshal(data, &key); err != nil {
		return nil, err
	}
	return key, nil
}
