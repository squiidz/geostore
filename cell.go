package geostore

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/golang/geo/s2"
)

type CellProcessor = func(*Cell) error

type Cell struct {
	CellID s2.CellID
	Items  []Item
	Hash   []byte
}

type storingCell struct {
	CellID s2.CellID
	Items  [][]byte
	Hash   []byte
}

func NewCell() *Cell {
	return &Cell{CellID: 0, Items: nil, Hash: nil}
}

func (c *Cell) key() []byte {
	return genCellKey(c.CellID)
}

func (c *Cell) addItem(itm Item) error {
	for _, it := range c.Items {
		if it.Key() == itm.Key() {
			return fmt.Errorf("item already exist in cell")
		}
	}
	c.Items = append(c.Items, itm)
	return c.genHash()
}

func encodeCell(c *Cell) ([]byte, error) {
	buffer := bytes.NewBuffer(nil)
	err := json.NewEncoder(buffer).Encode(c.storingCell())
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func decodeCell(cbyte []byte) (*Cell, error) {
	c := &storingCell{}
	err := json.Unmarshal(cbyte, c)
	if err != nil {
		return nil, err
	}
	return c.cell(), nil
}

func (c *Cell) process(cp CellProcessor) ([]byte, error) {
	err := cp(c)
	if err != nil {
		return nil, err
	}
	err = c.genHash()
	if err != nil {
		return nil, err
	}
	bCell, err := encodeCell(c)
	if err != nil {
		return nil, err
	}
	return bCell, nil
}

func (c *Cell) genHash() error {
	h := sha256.New()
	for _, itm := range c.Items {
		_, err := h.Write([]byte(itm.Key()))
		if err != nil {
			return err
		}
	}
	c.Hash = h.Sum(nil)
	return nil
}

func (c *Cell) storingCell() *storingCell {
	sc := &storingCell{CellID: c.CellID, Hash: c.Hash}
	for _, itm := range c.Items {
		itmEnc, err := itm.Encode()
		if err != nil {
			continue
		}
		sc.Items = append(sc.Items, itmEnc)
	}
	return sc
}

func (sc *storingCell) cell() *Cell {
	c := &Cell{CellID: sc.CellID, Hash: sc.Hash}
	for _, itm := range sc.Items {
		i, err := decoder(itm)
		if err != nil {
			continue
		}
		c.Items = append(c.Items, i)
	}
	return c
}
