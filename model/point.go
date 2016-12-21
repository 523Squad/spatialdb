package model

import "github.com/dhconnelly/rtreego"

// Point represents geographical point on the globe.
type Point struct {
	Location  *rtreego.SPoint
	Name      string
}
