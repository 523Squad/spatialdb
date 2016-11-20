package model

import (
	"fmt"

	"github.com/dhconnelly/rtreego"
)

// Point represents geographical point on the globe.
type Point struct {
	Latitude  float64
	Longitude float64
	Name      string
}

// Bounds implement Spatial interface for *Point.
func (p *Point) Bounds() *rtreego.Rect {
	if bounds, err := rtreego.NewRect(rtreego.Point{p.Latitude, p.Longitude}, []float64{0, 0}); err == nil {
		return bounds
	}
	return nil
}

// Equals implement Spatial interface for *Point.
func (p *Point) Equals(cmp rtreego.Comparable) (bool, error) {
	if that, ok := cmp.(*Point); ok {
		return p.Latitude == that.Latitude && p.Longitude == that.Longitude && p.Name == that.Name, nil
	}
	return false, fmt.Errorf("Wrong type of cmp")
}

func (p *Point) String() string {
	return fmt.Sprintf("%s, %.2f, %.2f", p.Name, p.Latitude, p.Longitude)
}
