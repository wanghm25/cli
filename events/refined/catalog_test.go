package refined

import "testing"

func TestRefinedKeys(t *testing.T) {
	var found bool
	for _, k := range Keys() {
		if k.Key == "im.message.created_v1" {
			found = true
			if !k.RefinedSubscription || len(k.KeyTemplates) != 2 {
				t.Fatalf("bad refined key: %+v", k)
			}
		}
	}
	if !found {
		t.Fatal("mock refined key im.message.created_v1 not in Keys()")
	}
}
