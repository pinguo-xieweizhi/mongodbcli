package main

import "testing"

func TestExtract(t *testing.T) {
	s, a := extractStyleAndAttribue("{\"style\":{\"opacity\":1},\"attribute\":{\"zIndex\":0,\"left\":0,\"borderRadius\":10}}")
	t.Log(s, a)
}
