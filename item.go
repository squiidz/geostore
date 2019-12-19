package geostore

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ItemErrUnbalancePositions = errors.New("length of positions coords is unbalance")
	ItemErrInvalidKey         = errors.New("invalid item key")
)

type ItemProcessor func(itm Item) Item

// Types that implement Item needs to have all their property public
type Item interface {
	// Lat, Lng
	Position() []float64
	// Key should not contains ":"
	Key() string
	Encode() ([]byte, error)
}

func isItemValid(itm Item) error {
	if len(itm.Position()) < 2 {
		return ItemErrUnbalancePositions
	}
	if len(itm.Position())%2 != 0 {
		return ItemErrUnbalancePositions
	}
	return nil
}

type ItemID string

func ItemIDFromStr(s string) (ItemID, error) {
	if !strings.Contains(s, ":") {
		return "", ItemErrInvalidKey
	}
	return ItemID(s), nil
}

func NewItemID(cellId uint64, key string) ItemID {
	return ItemID(fmt.Sprintf("%d:%s", cellId, key))
}

func (i ItemID) values() (cellId uint64, key string, err error) {
	ss := strings.Split(string(i), ":")
	cid, err := strconv.ParseUint(ss[0], 10, 64)
	if err != nil {
		return 0, "", err
	}
	return cid, ss[1], nil
}
