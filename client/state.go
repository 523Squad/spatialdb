package client

import (
	"github.com/dhconnelly/rtreego"
)

type state struct {
	tree    *rtreego.Rtree
	fileLen int64
}
