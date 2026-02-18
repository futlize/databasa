package hnsw

import "math"

// DistanceFunc computes the distance between two vectors.
// Lower values = more similar.
type DistanceFunc func(a, b []float32) float32

// QueryDistanceFunc computes the distance from one fixed query vector to a candidate vector.
type QueryDistanceFunc func(candidate []float32) float32

// QueryDistanceFactory prepares a QueryDistanceFunc for a specific query.
type QueryDistanceFactory func(query []float32) QueryDistanceFunc

// CosineDistance returns 1 - cosine_similarity(a, b).
// Range: [0, 2]. 0 = identical direction.
func CosineDistance(a, b []float32) float32 {
	var dot, normA, normB float32
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 1.0
	}
	sim := dot / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
	// Clamp to [-1, 1] to avoid NaN from floating point errors
	if sim > 1 {
		sim = 1
	} else if sim < -1 {
		sim = -1
	}
	return 1.0 - sim
}

// L2Distance returns the squared Euclidean distance between a and b.
// Using squared distance avoids the expensive sqrt and preserves ordering.
func L2Distance(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}

// DotProductDistance returns -dot(a, b).
// Negated so that higher dot product = lower distance (more similar).
func DotProductDistance(a, b []float32) float32 {
	var dot float32
	for i := range a {
		dot += a[i] * b[i]
	}
	return -dot
}

// PrepareCosineDistance returns a query-specialized cosine distance evaluator.
func PrepareCosineDistance(query []float32) QueryDistanceFunc {
	var normQ float32
	for _, v := range query {
		normQ += v * v
	}
	normQ = float32(math.Sqrt(float64(normQ)))
	if normQ == 0 {
		return func(_ []float32) float32 { return 1.0 }
	}

	return func(candidate []float32) float32 {
		var dot, normC float32
		for i := range query {
			v := candidate[i]
			dot += query[i] * v
			normC += v * v
		}
		if normC == 0 {
			return 1.0
		}
		sim := dot / (normQ * float32(math.Sqrt(float64(normC))))
		if sim > 1 {
			sim = 1
		} else if sim < -1 {
			sim = -1
		}
		return 1.0 - sim
	}
}

// PrepareL2Distance returns a query-specialized squared L2 evaluator.
func PrepareL2Distance(query []float32) QueryDistanceFunc {
	return func(candidate []float32) float32 {
		var sum float32
		for i := range query {
			d := query[i] - candidate[i]
			sum += d * d
		}
		return sum
	}
}

// PrepareDotProductDistance returns a query-specialized negative dot-product evaluator.
func PrepareDotProductDistance(query []float32) QueryDistanceFunc {
	return func(candidate []float32) float32 {
		var dot float32
		for i := range query {
			dot += query[i] * candidate[i]
		}
		return -dot
	}
}
