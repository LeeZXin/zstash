package model

type LogContent struct {
	Timestamp int64  `json:"timestamp"`
	Version   string `json:"version"`
	Level     string `json:"level"`
	Env       string `json:"env"`

	SourceIp string `json:"sourceIp"`
	Type     string `json:"type"`

	Application string `json:"application"`
	Content     string `json:"content"`
}
