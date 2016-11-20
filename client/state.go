package client

import (
	"spatialdb/index"
)

type state struct {
	tree *index.RTree
}
