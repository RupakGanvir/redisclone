package store

func matchGlob(pattern, s string) bool {
	p, si := 0, 0
	starIdx, matchIdx := -1, -1

	for si < len(s) {
		if p < len(pattern) && (pattern[p] == '?' || pattern[p] == s[si]) {
			p++
			si++
		} else if p < len(pattern) && pattern[p] == '*' {
			starIdx = p
			matchIdx = si
			p++
		} else if starIdx != -1 {
			p = starIdx + 1
			matchIdx++
			si = matchIdx
		} else {
			return false
		}
	}

	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}
