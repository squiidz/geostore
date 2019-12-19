package geostore

import (
	"encoding/binary"
	"math"
	"strconv"
)

func contains(arr []int, val int) bool {
	for _, a := range arr {
		if a == val {
			return true
		}
	}
	return false
}

func float64ToBytes(f float64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(f))
	return buf[:]
}

func float64fromBytes(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	float := math.Float64frombits(bits)
	return float
}

func uint32ToBytes(u uint32) []byte {
	str := strconv.Itoa(int(u))
	return []byte(str)
}

func uint32FromBytes(bytes []byte) uint32 {
	x, _ := strconv.Atoi(string(bytes))
	return uint32(x)
}

func uint64ToBytes(u uint64) []byte {
	str := strconv.Itoa(int(u))
	return []byte(str)
}

func uint64FromBytes(bytes []byte) uint64 {
	x, _ := strconv.Atoi(string(bytes))
	return uint64(x)
}

func int64ToBytes(u int64) []byte {
	str := strconv.Itoa(int(u))
	return []byte(str)
}

func int64FromBytes(bytes []byte) int64 {
	x, _ := strconv.Atoi(string(bytes))
	return int64(x)
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func intToBool(v int) bool {
	if v == 1 {
		return true
	}
	return false
}

func boolToBytes(b bool) []byte {
	v := boolToInt(b)
	s := strconv.Itoa(v)
	return []byte(s)
}

func boolFromBytes(b []byte) bool {
	n, _ := strconv.Atoi(string(b))
	return intToBool(n)
}
