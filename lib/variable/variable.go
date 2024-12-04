package variable

func GetDefault[T string](in T, def T) T {
	if in == "" {
		return def
	}
	return in
}
