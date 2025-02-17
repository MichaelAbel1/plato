package discovery

import (
	"encoding/json"
)

type EndpointInfo struct {
	IP       string                 `json:"ip"`
	Port     string                 `json:"port"`
	MetaData map[string]interface{} `json:"meta"`
}

func UnMarshal(data []byte) (*EndpointInfo, error) {
	ed := &EndpointInfo{}
	err := json.Unmarshal(data, ed) // 将一段 JSON 格式的字节数组（data）反序列化（unmarshal）
	if err != nil {
		return nil, err
	}
	return ed, nil
}
func (edi *EndpointInfo) Marshal() string {
	data, err := json.Marshal(edi) // 序列化为json格式
	if err != nil {
		panic(err)
	}
	return string(data)
}
