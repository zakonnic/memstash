package memstash

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyErrPicksTheKeysPart(t *testing.T) {
	down := errors.New("connection reset")
	a := &KeyError{Key: "a", Err: errors.New("bad a")}
	b := &KeyError{Key: "b", Err: errors.New("bad b")}

	assert.NoError(t, keyErr(nil, "a"))
	assert.Equal(t, down, keyErr(down, "a"), "an error naming no key concerns every key")

	named := errors.Join(a, errors.Join(b))
	assert.ErrorIs(t, keyErr(named, "a"), a)
	assert.ErrorIs(t, keyErr(named, "b"), b, "found inside a nested join")
	assert.NoError(t, keyErr(named, "c"), "a key the error does not name did not fail")

	mixed := errors.Join(a, down)
	assert.ErrorIs(t, keyErr(mixed, "a"), a)
	assert.ErrorIs(t, keyErr(mixed, "a"), down)
	assert.Equal(t, down, keyErr(mixed, "c"))
}
