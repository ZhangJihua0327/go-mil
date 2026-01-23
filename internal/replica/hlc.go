package replica

import (
	"sync"
	"time"
)

// HLC implements Hybrid Logical Clock
type HLC struct {
	mu      sync.Mutex
	wall    int64 // wall time in nanoseconds
	logical int32
}

// NewHLC creates a new HLC instance
func NewHLC() *HLC {
	return &HLC{
		wall:    time.Now().UnixNano(),
		logical: 0,
	}
}

// Now returns the current HLC timestamp packed into a uint64.
// Using 48 bits for wall time (milliseconds) and 16 bits for logical counter.
func (h *HLC) Now() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	physical := time.Now().UnixNano()

	if physical > h.wall {
		h.wall = physical
		h.logical = 0
	} else {
		h.logical++
	}

	return pack(h.wall, h.logical)
}

// Update updates the local HLC with a packed remote timestamp u
// returns the updated local timestamp packed into a uint64
func (h *HLC) Update(u uint64) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Unpack remote
	remoteWall, remoteLogical := unpack(u)

	physical := time.Now().UnixNano()

	newWall := physical
	if remoteWall > newWall {
		newWall = remoteWall
	}
	if h.wall > newWall {
		newWall = h.wall
	}

	if newWall == h.wall && newWall == remoteWall {
		if remoteLogical > h.logical {
			h.logical = remoteLogical + 1
		} else {
			h.logical++
		}
	} else if newWall == h.wall {
		h.logical++
	} else if newWall == remoteWall {
		h.logical = remoteLogical + 1
	} else {
		h.logical = 0
	}

	h.wall = newWall

	return pack(h.wall, h.logical)
}

// pack helper function
func pack(wall int64, logical int32) uint64 {
	// wall time in ms (UnixNano / 1e6)
	ms := uint64(wall / 1e6)
	return (ms << 16) | uint64(logical&0xFFFF)
}

// unpack helper function
func unpack(u uint64) (int64, int32) {
	wall := int64(u>>16) * 1e6
	logical := int32(u & 0xFFFF)
	return wall, logical
}
