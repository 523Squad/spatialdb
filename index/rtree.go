package index

import (
	"spatialdb/model"

	"github.com/dhconnelly/rtreego"
)

// TODO: Adjust those values
const minChildren = 25
const maxChildren = 50

// RTree is an index tree for geo data. Currently a proxy for github.com/dhconnelly/rtreego.
type RTree struct {
	rtree *rtreego.Rtree
}

func NewTree() *RTree {
	return &RTree{rtree: rtreego.NewTree(2, minChildren, maxChildren)}
}

func (tree *RTree) Insert(p *model.Point) {
	tree.rtree.Insert(p)
}
