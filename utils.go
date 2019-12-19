package geostore

import (
	"fmt"
	"math"

	"github.com/golang/geo/s2"
)

const earthCircumferenceMeter = 40075017

type GeoType int

const (
	GTPoint GeoType = iota
	GTPolyline
	GTPolygone
)

func radialAreaMeters(radius float64) float64 {
	nr := (radius / earthCircumferenceMeter) * 2 * math.Pi
	return math.Pi * nr * nr
}

func genItemKey(cellId s2.CellID, itm Item) []byte {
	return []byte(fmt.Sprintf("%x:%s", cellId, itm.Key()))
}

func genCellKey(cellId s2.CellID) []byte {
	return []byte(fmt.Sprintf("cell:%x", cellId))
}

type GeoMatch struct {
	CellIDs []s2.CellID
	GeoType GeoType
}

func MatchGeoType(itm Item, lvl int) (*GeoMatch, error) {
	if err := isItemValid(itm); err != nil {
		return nil, err
	}
	positions := itm.Position()
	if len(positions) == 2 {
		return &GeoMatch{CellIDs: []s2.CellID{MatchPoint(itm, lvl)}, GeoType: GTPoint}, nil
	}
	if positions[1] == positions[len(positions)-1] {
		return &GeoMatch{CellIDs: MatchPolygone(itm, lvl), GeoType: GTPolygone}, nil
	}
	return &GeoMatch{CellIDs: MatchPolyline(itm, lvl), GeoType: GTPolyline}, nil
}

func MatchPoint(itm Item, lvl int) s2.CellID {
	lat := itm.Position()[0]
	lng := itm.Position()[1]
	return matchCoord(lat, lng, lvl)
}

func MatchPolyline(itm Item, lvl int) []s2.CellID {
	cellIDs := []s2.CellID{}
	positions := itm.Position()
	for i := 0; i < len(positions)-1; i += 2 {
		cellIDs = append(cellIDs, matchCoord(positions[i], positions[i+1], lvl))
	}
	return cellIDs
}

func MatchPolygone(itm Item, lvl int) []s2.CellID {
	positions := itm.Position()
	var points []s2.Point
	cov := &s2.RegionCoverer{MinLevel: lvl, MaxLevel: lvl}
	for i := 0; i < len(positions)-1; i += 2 {
		points = append(points, s2.PointFromLatLng(s2.LatLngFromDegrees(positions[i], positions[i+1])))
	}
	loop := s2.LoopFromPoints(points)
	poly := s2.PolygonFromOrientedLoops([]*s2.Loop{loop})
	return cov.Covering(poly)
}

func matchCoord(lat, lng float64, lvl int) s2.CellID {
	cell := s2.CellFromLatLng(s2.LatLngFromDegrees(lat, lng))
	return cell.ID().Parent(lvl)
}
