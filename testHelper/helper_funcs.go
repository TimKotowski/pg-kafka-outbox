package testHelper

func GroupBy[T comparable, V any](elements []V, keySelector func(V) T) map[T][]V {
	destination := make(map[T][]V)
	for _, element := range elements {
		key := keySelector(element)
		destination[key] = append(destination[key], element)
	}

	return destination
}

// Partition returns a tuple of lists based on predicate.
// first slice is grouped where predicated yielded true.
// seconds slice is grouped where predicated yielded true.
func Partition[V any](elements []V, predicate func(V) bool) ([]V, []V) {
	var first []V
	var second []V
	for _, element := range elements {
		if predicate(element) {
			first = append(first, element)
		} else {
			second = append(second, element)
		}
	}

	return first, second
}
