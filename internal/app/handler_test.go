package app

import "testing"

func TestHandler(t *testing.T) {
	t.Run("hello world", func(t *testing.T) {
		if 1+1 != 2 {
			t.Errorf("Expected %d, but got %d", 2, 1+1)
		}
	})
}