package sketch

import (
	"math"
	"math/rand/v2"
	"reflect"
	"sort"
	"testing"
)

func exactQuantile(sorted []float64, q float64) float64 {
	return sorted[int(q*float64(len(sorted)-1))]
}

func TestQuantileRelativeErrorBound(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	distributions := map[string]func() float64{
		"lognormal-api":   func() float64 { return math.Exp(rng.NormFloat64()*0.6 + math.Log(80)) },
		"bimodal-cache":   func() float64 { return pick(rng, 0.9, 2+rng.Float64(), 300+rng.Float64()*200) },
		"uniform-wide":    func() float64 { return 0.5 + rng.Float64()*30000 },
		"heavy-tail":      func() float64 { return 10 / math.Pow(rng.Float64()+1e-9, 0.7) },
		"sub-millisecond": func() float64 { return 0.01 + rng.Float64()*0.5 },
	}
	for name, gen := range distributions {
		t.Run(name, func(t *testing.T) {
			for trial := range 20 {
				n := 1 + rng.IntN(5000)
				s := New()
				xs := make([]float64, n)
				for i := range xs {
					xs[i] = gen()
					s.Add(xs[i])
				}
				sort.Float64s(xs)
				for _, q := range []float64{0, 0.01, 0.25, 0.5, 0.9, 0.95, 0.99, 0.999, 1} {
					got, want := s.Quantile(q), exactQuantile(xs, q)
					if rel := math.Abs(got-want) / want; rel > Accuracy+1e-9 {
						t.Fatalf("trial %d n=%d q=%v: got %v want %v (rel err %.4f > %.4f)", trial, n, q, got, want, rel, Accuracy)
					}
				}
			}
		})
	}
}

func pick(rng *rand.Rand, p, a, b float64) float64 {
	if rng.Float64() < p {
		return a
	}
	return b
}

func TestMergeEqualsUnion(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for range 50 {
		a, b, union := New(), New(), New()
		for range rng.IntN(500) {
			v := rng.ExpFloat64() * 120
			a.Add(v)
			union.Add(v)
		}
		for range rng.IntN(500) {
			v := rng.ExpFloat64() * 40
			b.Add(v)
			union.Add(v)
		}
		a.Merge(b)
		if a.Count() != union.Count() || !reflect.DeepEqual(a.bins, union.bins) || a.zero != union.zero {
			t.Fatalf("merge differs from union: %d vs %d samples", a.Count(), union.Count())
		}
		for _, q := range []float64{0.5, 0.95, 0.99} {
			if a.Quantile(q) != union.Quantile(q) {
				t.Fatalf("q%v: merged %v union %v", q, a.Quantile(q), union.Quantile(q))
			}
		}
	}
}

func TestEmptySketch(t *testing.T) {
	var s Sketch
	if !math.IsNaN(s.Quantile(0.5)) || !math.IsNaN(s.Mean()) || !math.IsNaN(s.Max()) {
		t.Fatal("empty sketch should report NaN")
	}
	s.Merge(nil)
	s.Merge(New())
	if s.Count() != 0 {
		t.Fatal("merging empty sketches added samples")
	}
}

func TestBinaryRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	s := New()
	for range 2000 {
		s.Add(rng.ExpFloat64() * 90)
	}
	s.Add(0)
	s.AddN(42, 7)
	data, err := s.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var got Sketch
	if err := got.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&got, s) {
		t.Fatal("round trip changed the sketch")
	}
	if len(data) > 1200 {
		t.Errorf("encoding is %d bytes; expected a compact sparse encoding", len(data))
	}
}

func TestUnmarshalRejectsCorruptInput(t *testing.T) {
	s := New()
	s.Add(10)
	s.Add(20)
	data, _ := s.MarshalBinary()
	cases := map[string][]byte{
		"bad version": append([]byte{9}, data[1:]...),
		"truncated":   data[:len(data)-1],
		"trailing":    append(append([]byte{}, data...), 0),
	}
	for name, b := range cases {
		var got Sketch
		if err := got.UnmarshalBinary(b); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func FuzzUnmarshal(f *testing.F) {
	s := New()
	for _, v := range []float64{0, 1, 5, 250, 9000} {
		s.Add(v)
	}
	seed, _ := s.MarshalBinary()
	f.Add(seed)
	f.Add([]byte{1})
	f.Fuzz(func(t *testing.T, data []byte) {
		var got Sketch
		if err := got.UnmarshalBinary(data); err != nil {
			return
		}
		// Anything that decodes must be internally consistent and re-encode.
		_ = got.Quantile(0.95)
		if _, err := got.MarshalBinary(); err != nil {
			t.Fatal(err)
		}
	})
}

func BenchmarkAdd(b *testing.B) {
	s := New()
	rng := rand.New(rand.NewPCG(7, 8))
	vals := make([]float64, 1024)
	for i := range vals {
		vals[i] = rng.ExpFloat64() * 100
	}
	b.ResetTimer()
	for i := range b.N {
		s.Add(vals[i&1023])
	}
}

func BenchmarkMergeAndQuantile24Hours(b *testing.B) {
	rng := rand.New(rand.NewPCG(9, 10))
	hours := make([]*Sketch, 24)
	for i := range hours {
		hours[i] = New()
		for range 120 {
			hours[i].Add(rng.ExpFloat64() * 100)
		}
	}
	b.ResetTimer()
	for range b.N {
		day := New()
		for _, h := range hours {
			day.Merge(h)
		}
		_ = day.Quantile(0.95)
	}
}
