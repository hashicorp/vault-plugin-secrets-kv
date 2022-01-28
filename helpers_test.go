package kv

// getKeySet will produce a set of that of keys that exist in m
func getKeySet(m map[string]interface{}) map[string]struct{} {
	set := make(map[string]struct{})

	for k := range m {
		set[k] = struct{}{}
	}

	return set
}

// expectedMetadataKeys produces a deterministic set of expected
// metadata keys to ensure consistent shape across all endpoints
func expectedMetadataKeys() map[string]struct{} {
	return map[string]struct{}{
		"version":         {},
		"created_time":    {},
		"deletion_time":   {},
		"destroyed":       {},
		"custom_metadata": {},
	}
}
