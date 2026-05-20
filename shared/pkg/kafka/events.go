package kafka

type DriverLocationEvent struct {
	EventID   string  `json:"event_id"`
	EventType string  `json:"event_type"`
	DriverID  string  `json:"driver_id"`
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	Available bool    `json:"available"`
	Timestamp string  `json:"timestamp"`
}
