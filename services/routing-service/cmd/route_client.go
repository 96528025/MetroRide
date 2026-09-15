package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"io"
	"math"
	"net/http"
	"strings"
	"time"
)

type routeEstimate struct {
	DistanceKM      float64 `json:"distance_km"`
	DurationSeconds float64 `json:"duration_seconds"`
	Provider        string  `json:"provider"`
	CalculatedAt    string  `json:"calculated_at"`
}
type routeEstimator interface {
	Estimate(context.Context, float64, float64, float64, float64) (routeEstimate, error)
}
type valhallaClient struct {
	base   string
	client *http.Client
	rdb    *redis.Client
}

func (c *valhallaClient) Estimate(ctx context.Context, lat, lon, dlat, dlon float64) (routeEstimate, error) {
	if !validPoint(lat, lon) || !validPoint(dlat, dlon) {
		return routeEstimate{}, errors.New("invalid route coordinates")
	}
	raw, _ := json.Marshal(map[string]any{"locations": []map[string]float64{{"lat": lat, "lon": lon}, {"lat": dlat, "lon": dlon}}, "costing": "auto", "units": "kilometers"})
	digest := sha256.Sum256(append([]byte(c.base), raw...))
	key := "route-cache:" + hex.EncodeToString(digest[:])
	owner := uuid.NewString()
	for {
		cached, e := c.rdb.Get(ctx, key).Bytes()
		if e == nil {
			var route routeEstimate
			if json.Unmarshal(cached, &route) == nil {
				return route, nil
			}
		} else if !errors.Is(e, redis.Nil) {
			return routeEstimate{}, e
		}
		locked, e := c.rdb.SetNX(ctx, key+":lock", owner, 75*time.Second).Result()
		if e != nil {
			return routeEstimate{}, e
		}
		if locked {
			break
		}
		if !routeWait(ctx, 50*time.Millisecond) {
			return routeEstimate{}, ctx.Err()
		}
	}
	defer func() {
		release, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		c.rdb.Eval(release, `if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) end return 0`, []string{key + ":lock"}, owner)
	}()
	// Share request-start spacing across routing replicas using the same Redis.
	host := sha256.Sum256([]byte(c.base))
	rateKey := fmt.Sprintf("route-rate:%x", host)
	for {
		ok, e := c.rdb.SetNX(ctx, rateKey, "1", time.Second).Result()
		if e != nil {
			return routeEstimate{}, e
		}
		if ok {
			break
		}
		if !routeWait(ctx, 100*time.Millisecond) {
			return routeEstimate{}, ctx.Err()
		}
	}
	route, e := c.fetch(ctx, raw)
	if e != nil {
		return routeEstimate{}, e
	}
	encoded, _ := json.Marshal(route)
	if e = c.rdb.Set(ctx, key, encoded, time.Minute).Err(); e != nil {
		return routeEstimate{}, e
	}
	return route, nil
}
func (c *valhallaClient) fetch(ctx context.Context, body []byte) (routeEstimate, error) {
	call, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, e := http.NewRequestWithContext(call, http.MethodPost, strings.TrimRight(c.base, "/")+"/route", bytes.NewReader(body))
	if e != nil {
		return routeEstimate{}, e
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "MetroRide/1.0")
	resp, e := c.client.Do(req)
	if e != nil {
		return routeEstimate{}, fmt.Errorf("route provider unavailable: %w", e)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return routeEstimate{}, fmt.Errorf("route provider returned HTTP %d", resp.StatusCode)
	}
	var wire struct {
		Trip struct {
			Status  *int   `json:"status"`
			Units   string `json:"units"`
			Summary struct {
				Length *float64 `json:"length"`
				Time   *float64 `json:"time"`
			} `json:"summary"`
		} `json:"trip"`
	}
	if e = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&wire); e != nil {
		return routeEstimate{}, errors.New("invalid route response")
	}
	t := wire.Trip
	if t.Status == nil || *t.Status != 0 || t.Units != "kilometers" || t.Summary.Length == nil || t.Summary.Time == nil {
		return routeEstimate{}, errors.New("incomplete route response or incorrect units")
	}
	distance, duration := *t.Summary.Length, *t.Summary.Time
	if distance < 0 || duration < 0 || math.IsNaN(distance) || math.IsNaN(duration) || math.IsInf(distance, 0) || math.IsInf(duration, 0) {
		return routeEstimate{}, errors.New("invalid route measurements")
	}
	return routeEstimate{distance, duration, c.base, time.Now().UTC().Format(time.RFC3339Nano)}, nil
}
func routeWait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
