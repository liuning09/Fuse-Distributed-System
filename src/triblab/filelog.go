package triblab

import (
	"encoding/json"
)
// TODO: create logging format

type FileLog struct {
	Key		string
	Time	string
	Op		string
	Offset	uint64
	Data	string
}

func EnocodeFileLog(log FileLog) string {
	b, _ := json.Marshal(log)
	return string(b)
}

func DecodeFileLog(value string) *FileLog {
	var log FileLog
	json.Unmarshal([]byte(value), &log)
	return &log
}
