package model

import "github.com/dhconnelly/rtreego"

// Point represents geographical point on the globe.
type Point struct {
	ID       int64
	Location *rtreego.SPoint
	Name     string
}

// Int64Slice attaches the methods of Interface to []int64, sorting in increasing order.
type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
