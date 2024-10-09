package common

type JoinCache[K comparable, V any] struct {
	cache map[K]V

	limit int
}

func NewJoinCache[K comparable, V any](limit int) *JoinCache[K, V] {
	return &JoinCache[K, V]{
		cache: make(map[K]V),
		limit: limit,
	}
}

func (c *JoinCache[K, V]) TryPut(key K, value V) bool {
	if len(c.cache) > c.limit {
		return false
	}
	c.cache[key] = value
	return true
}

func (c *JoinCache[K, V]) Get(key K) (V, bool) {
	v, ok := c.cache[key]
	return v, ok
}

func (c *JoinCache[K, V]) Remove(key K) {
	delete(c.cache, key)
}
