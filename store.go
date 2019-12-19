package geostore

import (
	"fmt"

	"github.com/dgraph-io/badger/v2"
	"github.com/golang/geo/s2"
)

const (
	open storeState = iota
	close
)

var decoder Decoder

type Decoder func([]byte) (Item, error)
type storeState int

type Store struct {
	cellLvl int
	state   storeState
	dbpath  string
	DB      *badger.DB
}

func NewStore(dbpath string, cellLvl int) *Store {
	return &Store{
		cellLvl: cellLvl,
		state:   close,
		dbpath:  dbpath,
		DB:      nil,
	}
}

func (s *Store) Open(dec Decoder) error {
	opts := badger.DefaultOptions(s.dbpath)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.DB = db
	s.state = open
	decoder = dec
	return nil
}

func (s *Store) isOpen() bool {
	if s.state == open {
		return true
	}
	return false
}

func (s *Store) Close() error {
	return s.DB.Close()
}

// TODO: create a second index to store item cellIds
func (s *Store) Insert(itm Item) (ItemID, error) {
	if !s.isOpen() {
		return "", fmt.Errorf("store not open, call .Open() on store instance before inserting")
	}
	if err := isItemValid(itm); err != nil {
		return "", err
	}
	batch := s.DB.NewWriteBatch()
	gm, err := MatchGeoType(itm, s.cellLvl)
	if err != nil {
		return "", err
	}
	for _, cid := range gm.CellIDs {
		c, err := s.GetCell(uint64(cid))
		if err != nil {
			c = &Cell{CellID: cid}
		}
		if err := c.addItem(itm); err != nil {
			return "", err
		}
		encCell, err := encodeCell(c)
		if err != nil {
			return "", err
		}
		err = batch.Set(c.key(), encCell)
		if err != nil {
			return "", err
		}
	}
	if err := batch.Flush(); err != nil {
		return "", err
	}
	return NewItemID(uint64(gm.CellIDs[0]), itm.Key()), nil
}

func (s *Store) Get(itmID ItemID) (Item, error) {
	if !s.isOpen() {
		return nil, fmt.Errorf("store not open, call .Open() on store instance before getting")
	}
	cid, k, err := itmID.values()
	if err != nil {
		return nil, err
	}
	ck := genCellKey(s2.CellID(cid))
	c := NewCell()
	err = s.DB.View(func(txn *badger.Txn) error {
		itm, err := txn.Get(ck)
		if err != nil {
			return err
		}
		return itm.Value(func(val []byte) error {
			c, err = decodeCell(val)
			return err
		})
	})
	if err != nil {
		return nil, err
	}
	for _, it := range c.Items {
		if it.Key() == k {
			return it, nil
		}
	}
	return nil, fmt.Errorf("key %s not found", k)
}

func (s *Store) GetByPrefix(prefix string) ([]Item, error) {
	bp := []byte(prefix)
	items := []Item{}
	txn := s.DB.NewTransaction(false)
	itr := txn.NewIterator(badger.DefaultIteratorOptions)
	itr.Seek(bp)
	for itr.ValidForPrefix(bp) {
		bitm := itr.Item()
		err := bitm.Value(func(val []byte) error {
			itm, err := decoder(val)
			if err != nil {
				return err
			}
			items = append(items, itm)
			return nil
		})
		if err != nil {
			return nil, err
		}
		itr.Next()
	}
	return items, nil
}

func (s *Store) GetCell(cellID uint64) (*Cell, error) {
	if !s.isOpen() {
		return nil, fmt.Errorf("store not open, call .Open() on store instance before getting")
	}
	txn := s.DB.NewTransaction(false)
	c := NewCell()
	ck := genCellKey(s2.CellID(cellID))
	itm, err := txn.Get(ck)
	if err != nil {
		return nil, err
	}
	err = itm.Value(func(val []byte) error {
		c, err = decodeCell(val)
		return err
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (s *Store) GetCellByLatLng(lat, lng float64) (*Cell, error) {
	return s.GetCell(uint64(matchCoord(lat, lng, s.cellLvl)))
}

func (s *Store) WithinRadius(lat, lng float64, radius float64) ([]*Cell, error) {
	point := s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng))
	cap := s2.CapFromCenterArea(point, radialAreaMeters(radius))
	cov := &s2.RegionCoverer{MinLevel: s.cellLvl, MaxLevel: s.cellLvl}
	cu := cov.Covering(cap)
	cells := []*Cell{}
	for _, cid := range cu {
		c, err := s.GetCell(uint64(cid))
		if err != nil {
			return nil, err
		}
		cells = append(cells, c)
	}
	return cells, nil
}

func (s *Store) Update(itmID ItemID, ip ItemProcessor) error {
	return s.processItem(itmID, ip)
}

func (s *Store) Delete(itmId ItemID) error {
	return s.processCell(itmId, func(c *Cell) error {
		for i, itm := range c.Items {
			_, k, err := itmId.values()
			if err != nil {
				return err
			}
			if itm.Key() == k {
				c.Items = append(c.Items[:i], c.Items[i+1:]...)
				return nil
			}
		}
		return fmt.Errorf("item %s not found", itmId)
	})
}

func (s *Store) processCell(itmID ItemID, fn CellProcessor) error {
	itm, err := s.Get(itmID)
	if err != nil {
		return err
	}
	gm, err := MatchGeoType(itm, s.cellLvl)
	if err != nil {
		return err
	}
	if len(gm.CellIDs) == 0 {
		return fmt.Errorf("no cell found for %s", itm.Key())
	}
	batch := s.DB.NewWriteBatch()
	for _, cid := range gm.CellIDs {
		cell, err := s.GetCell(uint64(cid))
		if err != nil {
			return err
		}
		bCell, err := cell.process(fn)
		if err != nil {
			return err
		}
		err = batch.Set(genCellKey(cid), bCell)
		if err != nil {
			return err
		}
	}
	return batch.Flush()
}

func (s *Store) processItem(itmID ItemID, ip ItemProcessor) error {
	return s.processCell(itmID, func(c *Cell) error {
		for i, itm := range c.Items {
			_, k, err := itmID.values()
			if err != nil {
				return err
			}
			if itm.Key() == k {
				c.Items[i] = ip(itm)
			}
		}
		return nil
	})
}
