// Package sketch implements a mergeable latency histogram with bounded
// relative error, in the style of DDSketch.
//
// Percentiles cannot be averaged: the mean of 24 hourly p95 values is not
// the daily p95. Rollups therefore store a sketch per bucket, and sketches
// merge by adding bucket counts, which is exact. Any quantile read from a
// merged sketch is within Accuracy (relative) of the true sample quantile.
package sketch

import (
	"encoding/binary"
	"errors"
	"math"
	"sort"
)

// Accuracy is the guaranteed relative error of a quantile estimate.
const Accuracy = 0.02

var (
	gamma    = (1 + Accuracy) / (1 - Accuracy)
	logGamma = math.Log(gamma)
)

// minValue is the smallest latency (in milliseconds) that gets its own
// bucket. Anything at or below it is counted in the zero bucket; a probe that
// measures 1µs has measured nothing meaningful.
const minValue = 0.001

// Sketch accumulates latency samples in milliseconds. The zero value is ready
// to use.
type Sketch struct {
	bins  map[int32]uint64
	zero  uint64
	count uint64
	sum   float64
	min   float64
	max   float64
}

// New returns an empty sketch.
func New() *Sketch { return &Sketch{} }

// Add records one sample.
func (s *Sketch) Add(ms float64) { s.AddN(ms, 1) }

// AddN records n identical samples.
func (s *Sketch) AddN(ms float64, n uint64) {
	if n == 0 || math.IsNaN(ms) {
		return
	}
	if s.count == 0 || ms < s.min {
		s.min = ms
	}
	if s.count == 0 || ms > s.max {
		s.max = ms
	}
	s.count += n
	s.sum += ms * float64(n)
	if ms <= minValue {
		s.zero += n
		return
	}
	if s.bins == nil {
		s.bins = make(map[int32]uint64)
	}
	s.bins[index(ms)] += n
}

// Merge adds every sample of o into s.
func (s *Sketch) Merge(o *Sketch) {
	if o == nil || o.count == 0 {
		return
	}
	if s.count == 0 || o.min < s.min {
		s.min = o.min
	}
	if s.count == 0 || o.max > s.max {
		s.max = o.max
	}
	s.count += o.count
	s.sum += o.sum
	s.zero += o.zero
	if len(o.bins) > 0 && s.bins == nil {
		s.bins = make(map[int32]uint64, len(o.bins))
	}
	for k, v := range o.bins {
		s.bins[k] += v
	}
}

// Count is the number of samples recorded.
func (s *Sketch) Count() uint64 { return s.count }

// Mean is the arithmetic mean, or NaN when empty.
func (s *Sketch) Mean() float64 {
	if s.count == 0 {
		return math.NaN()
	}
	return s.sum / float64(s.count)
}

// Max is the largest sample seen, or NaN when empty.
func (s *Sketch) Max() float64 {
	if s.count == 0 {
		return math.NaN()
	}
	return s.max
}

// Quantile returns the estimated q-quantile (0 <= q <= 1) in milliseconds,
// or NaN when the sketch is empty.
func (s *Sketch) Quantile(q float64) float64 {
	if s.count == 0 || q < 0 || q > 1 {
		return math.NaN()
	}
	// Lower-rank definition: the smallest sample with at least q*(n-1)
	// samples below it.
	rank := uint64(q * float64(s.count-1))
	if rank < s.zero {
		return s.min
	}
	seen := s.zero
	keys := s.sortedKeys()
	for _, k := range keys {
		seen += s.bins[k]
		if seen > rank {
			return clamp(value(k), s.min, s.max)
		}
	}
	return s.max
}

func (s *Sketch) sortedKeys() []int32 {
	keys := make([]int32, 0, len(s.bins))
	for k := range s.bins {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func index(ms float64) int32 { return int32(math.Ceil(math.Log(ms) / logGamma)) }

// value is the representative of bucket k: the point whose relative distance
// to both bucket edges is Accuracy.
func value(k int32) float64 { return 2 * math.Pow(gamma, float64(k)) / (gamma + 1) }

func clamp(v, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, v)) }

const encodingVersion = 1

// MarshalBinary encodes the sketch compactly: a header followed by
// delta-encoded bucket indexes and their counts as varints. A typical hourly
// latency distribution encodes to a few hundred bytes.
func (s *Sketch) MarshalBinary() ([]byte, error) {
	keys := s.sortedKeys()
	buf := make([]byte, 0, 40+len(keys)*4)
	buf = append(buf, encodingVersion)
	buf = binary.AppendUvarint(buf, s.count)
	buf = binary.AppendUvarint(buf, s.zero)
	buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(s.sum))
	buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(s.min))
	buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(s.max))
	buf = binary.AppendUvarint(buf, uint64(len(keys)))
	prev := int32(0)
	for _, k := range keys {
		buf = binary.AppendVarint(buf, int64(k-prev))
		buf = binary.AppendUvarint(buf, s.bins[k])
		prev = k
	}
	return buf, nil
}

// ErrCorrupt is returned when decoding a malformed sketch.
var ErrCorrupt = errors.New("sketch: corrupt encoding")

// UnmarshalBinary decodes data produced by MarshalBinary.
func (s *Sketch) UnmarshalBinary(data []byte) error {
	*s = Sketch{}
	if len(data) == 0 {
		return nil
	}
	if data[0] != encodingVersion {
		return ErrCorrupt
	}
	r := reader{b: data[1:]}
	count := r.uvarint()
	zero := r.uvarint()
	sum := math.Float64frombits(r.u64())
	lo := math.Float64frombits(r.u64())
	hi := math.Float64frombits(r.u64())
	n := r.uvarint()
	if r.err || n > uint64(len(r.b)) {
		return ErrCorrupt
	}
	bins := make(map[int32]uint64, n)
	total := zero
	k := int64(0)
	for range n {
		k += r.varint()
		c := r.uvarint()
		if r.err || k < math.MinInt32 || k > math.MaxInt32 {
			return ErrCorrupt
		}
		bins[int32(k)] += c
		total += c
	}
	if r.err || len(r.b) != 0 || total != count {
		return ErrCorrupt
	}
	s.count, s.zero, s.sum, s.min, s.max = count, zero, sum, lo, hi
	if len(bins) > 0 {
		s.bins = bins
	}
	return nil
}

type reader struct {
	b   []byte
	err bool
}

func (r *reader) uvarint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = true
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) varint() int64 {
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.err = true
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) u64() uint64 {
	if len(r.b) < 8 {
		r.err = true
		return 0
	}
	v := binary.LittleEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v
}
