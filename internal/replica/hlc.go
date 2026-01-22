package replica

import (
	"sync"
	"time"
)

// HLCTimestamp represents a Hybrid Logical Clock timestamp
type HLCTimestamp struct {
	WallTime int64 `json:"wall_time"`
	Logical  int32 `json:"logical"`
}

// HLC implements Hybrid Logical Clock
type HLC struct {
	mu      sync.Mutex
	wall    int64
	logical int32
}

// NewHLC creates a new HLC instance
func NewHLC() *HLC {
	return &HLC{
		wall:    time.Now().UnixNano(),
		logical: 0,
	}
}

// Now returns the current HLC timestamp (used for local events)
func (h *HLC) Now() HLCTimestamp {
	h.mu.Lock()
	defer h.mu.Unlock()

	physical := time.Now().UnixNano()

	if physical > h.wall {
		h.wall = physical
		h.logical = 0
	} else {
		h.logical++
	}

	return HLCTimestamp{
		WallTime: h.wall,
		Logical:  h.logical,
	}
}

// Update updates the local HLC with a remote timestamp t
// returns the updated local timestamp
func (h *HLC) Update(t HLCTimestamp) HLCTimestamp {
	h.mu.Lock()
	defer h.mu.Unlock()

	physical := time.Now().UnixNano()

	newWall := physical
	if t.WallTime > newWall {
		newWall = t.WallTime
	}
	if h.wall > newWall {
		newWall = h.wall
	}

	if newWall == h.wall && newWall == t.WallTime {
		if t.Logical > h.logical {
			h.logical = t.Logical + 1
		} else {
			h.logical++
		}
	} else if newWall == h.wall {
		h.logical++
	} else if newWall == t.WallTime {
		h.logical = t.Logical + 1
	} else {
		h.logical = 0
	}

	h.wall = newWall

	return HLCTimestamp{
		WallTime: h.wall,
		Logical:  h.logical,
	}
}
