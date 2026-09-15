// A deterministic Valhalla contract fixture for local/CI integration tests.
// It does not compute a road route and is never enabled by production defaults.
package main

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
)

func main() {
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	http.HandleFunc("/route", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Locations []struct {
				Lat float64 `json:"lat"`
				Lon float64 `json:"lon"`
			}
			Costing, Units string
		}
		if r.Method != "POST" || json.NewDecoder(r.Body).Decode(&req) != nil || len(req.Locations) != 2 || req.Costing != "auto" || req.Units != "kilometers" {
			http.Error(w, "invalid fixture request", 400)
			return
		}
		for _, p := range req.Locations {
			if math.Abs(p.Lat) > 90 || math.Abs(p.Lon) > 180 {
				http.Error(w, "invalid coordinates", 400)
				return
			}
		}
		// Distinct passenger and approach figures let end-to-end tests detect a mix-up.
		distance, duration := 1.25, 180.0
		if math.Abs(req.Locations[1].Lat-37.789) < .00001 && math.Abs(req.Locations[1].Lon+122.401) < .00001 {
			distance, duration = 8.5, 920.5
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"trip": map[string]any{"status": 0, "units": "kilometers", "summary": map[string]float64{"length": distance, "time": duration}}})
	})
	log.Fatal(http.ListenAndServe(":8090", nil))
}
