package vec

import "math"

// Sqrt32 returns the square root of x as float32.
func Sqrt32(x float32) float32 { return float32(math.Sqrt(float64(x))) }

// Dot returns the dot product of vectors a and b.
func Dot(a, b []float32) float32 {
	var s float32
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}

// Norm L2-normalizes a vector (in-place copy; v is not mutated).
func Norm(v []float32) []float32 {
	s := float32(1.0) / (Sqrt32(Dot(v, v)) + 1e-9)
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = x * s
	}
	return out
}
