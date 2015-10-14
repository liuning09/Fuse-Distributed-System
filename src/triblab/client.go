package triblab

import (
	"trib"
)

// client constructor
/*func NewClient(addr string) trib.Storage {
	panic("TODO")
}
*/
// bin client constructor
func NewBinClient(backs []string) trib.BinStorage {
	rClient := &ReplicaBinClient{backs, make(map[string]bool)}
	rClient.FindChord()
	return rClient
}
