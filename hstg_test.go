package hstg

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestItEncodesAndDecodesValuesWithADefaultEncoder(t *testing.T) {

	_, err := newDefaultCodec(0)
	require.Errorf(t, err, "an error was not raised when creating a default codec with bin width 0")

	h, err := newDefaultCodec(1)
	require.Nilf(t, err, "an error was returned when creatind a default codec with bin width 1: %s", err)
	require.NotNil(t, h, "nil NewDefaultCodec returned when trying to create codec with bin width 1")

	table := [][]uint{
		[]uint{0, 0, 0},
		[]uint{1, 1, 1},
		[]uint{2, 2, 2},
		[]uint{3, 3, 3},
		[]uint{4, 4, 4},
	}
	for i := 0; i < len(table); i++ {
		actual := h.encode(table[i][0])
		require.Equalf(t, actual, table[i][1], "encode error i = %d", i)
		actual = h.decode(table[i][1])
		require.Equalf(t, actual, table[i][2], "decode error i = %d", i)
	}

	h, err = newDefaultCodec(2)
	require.Nilf(t, err, "an error was returned when creatind a default codec with bin width 2: %s", err)
	require.NotNil(t, h, "nil NewDefaultCodec returned when trying to create codec with bin width 2")

	table = [][]uint{
		[]uint{0, 0, 0},
		[]uint{1, 0, 0},
		[]uint{2, 1, 2},
		[]uint{3, 1, 2},
		[]uint{4, 2, 4},
	}
	for i := 0; i < len(table); i++ {
		actual := h.encode(table[i][0])
		require.Equalf(t, actual, table[i][1], "encode error i = %d", i)
		actual = h.decode(table[i][1])
		require.Equalf(t, actual, table[i][2], "decode error i = %d", i)
	}
}

func TestItEncodesAndDecodesValuesWithAExpEncoder(t *testing.T) {

	_, err := newExpCodec(0)
	require.Errorf(t, err, "an error was not raised when creating an exponential codec with log base 0")
	_, err = newExpCodec(1)
	require.Errorf(t, err, "an error was not raised when creating an exponential codec with log base 1")
	h, err := newExpCodec(2)
	require.Nilf(t, err, "an error occurred creating an exponential codec with base 2: %s", err)
	table := [][]uint{
		[]uint{0, 0, 0},
		[]uint{1, 1, 1},
		[]uint{2, 1, 1},
		[]uint{3, 2, 3},
		[]uint{4, 2, 3},
		[]uint{5, 2, 3},
		[]uint{6, 2, 3},
		[]uint{7, 3, 7},
		[]uint{8, 3, 7},
		[]uint{9, 3, 7},
		[]uint{10, 3, 7},
		[]uint{11, 3, 7},
		[]uint{12, 3, 7},
		[]uint{13, 3, 7},
		[]uint{14, 3, 7},
		[]uint{15, 4, 15},
	}
	for i := 0; i < len(table); i++ {
		actual := h.encode(table[i][0])
		require.Equalf(t, actual, table[i][1], "encode error i = %d", i)
		actual = h.decode(table[i][1])
		require.Equalf(t, actual, table[i][2], "decode error i = %d", i)
	}

}

func TestItUpdatesTheBinList(t *testing.T) {
	list := newBinList()

	list.update(5, 1)
	require.Equalf(t, uint(1), list.totalFreq, "update error during empty insertion")
	require.Equalf(t, uint(1), list.length, "update error during empty insertion")
	require.Equalf(t, uint(5), list.curr.value, "update error during empty insertion")
	require.Equalf(t, uint(1), list.curr.freq, "update error during empty insertion")

	list.update(3, 1)
	require.Equalf(t, uint(2), list.totalFreq, "update error during head insertion")
	require.Equalf(t, uint(2), list.length, "update error during head insertion")
	require.Equalf(t, uint(3), list.curr.value, "update error during head insertion")
	require.Equalf(t, uint(1), list.curr.freq, "update error during head insertion")

	list.update(7, 1)
	require.Equalf(t, uint(3), list.totalFreq, "update error during tail insertion")
	require.Equalf(t, uint(3), list.length, "update error during tail insertion")
	require.Equalf(t, uint(7), list.curr.value, "update error during tail insertion")
	require.Equalf(t, uint(1), list.curr.freq, "update error during tail insertion")

	list.update(6, 1)
	require.Equalf(t, uint(4), list.totalFreq, "update error during middle insertion")
	require.Equalf(t, uint(4), list.length, "update error during middle insertion")
	require.Equalf(t, uint(6), list.curr.value, "update error during middle insertion")
	require.Equalf(t, uint(1), list.curr.freq, "update error during middle insertion")

	list.update(5, 2)
	require.Equalf(t, uint(4+2), list.totalFreq, "update error during existing search") // total freq is updated by 2
	require.Equalf(t, uint(4+0), list.length, "update error during existing search")    // list size does not increase
	require.Equalf(t, uint(5), list.curr.value, "update error during existing search")
	require.Equalf(t, uint(1+2), list.curr.freq, "update error during existing search") // bin freq is updated by 2

}

func TestItIteratesOverBins(t *testing.T) {
	list := newBinList()
	prank := float64(0)
	for _, i := range []uint{1, 2, 3, 4, 5} {
		list.update(i, i*i)
	}

	codec, _ := newDefaultCodec(1)
	iter := list.iter(codec)

	require.Equalf(t, false, iter.Done(), "iteration error - iterator done")
	require.Equalf(t, uint(1), iter.Percentile(), "iteration error - incorrect total frequency")
	prank = float64(0) / float64(list.totalFreq) * 100.0
	require.Equalf(t, prank, iter.PRank(), "iteration error: %v != %v", prank, iter.PRank())

	iter.Next()
	require.Equalf(t, false, iter.Done(), "iteration error - iterator done")
	require.Equalf(t, uint(2), iter.Percentile(), "iteration error - incorrect total frequency")
	prank = float64(1) / float64(list.totalFreq) * 100.0
	require.InEpsilonf(t, prank, iter.PRank(), 0.000001, "iteration error: %v != %v", prank, iter.PRank())

	iter.Next()
	require.Equalf(t, false, iter.Done(), "iteration error - iterator done")
	require.Equalf(t, uint(3), iter.Percentile(), "iteration error - incorrect total frequency")
	prank = float64(1+2*2) / float64(list.totalFreq) * 100.0
	require.InEpsilonf(t, prank, iter.PRank(), 0.000001, "iteration error: %v != %v", prank, iter.PRank())

	iter.Next()
	require.Equalf(t, false, iter.Done(), "iteration error - iterator done")
	require.Equalf(t, uint(4), iter.Percentile(), "iteration error - incorrect total frequency")
	prank = float64(1+2*2+3*3) / float64(list.totalFreq) * 100.0
	require.InEpsilonf(t, prank, iter.PRank(), 0.000001, "iteration error: %v != %v", prank, iter.PRank())

	iter.Next()
	require.Equalf(t, false, iter.Done(), "iteration error - iterator done")
	require.Equalf(t, uint(5), iter.Percentile(), "iteration error - incorrect total frequency")
	prank = float64(1+2*2+3*3+4*4) / float64(list.totalFreq) * 100.0
	require.InEpsilonf(t, prank, iter.PRank(), 0.000001, "iteration error: %v != %v", prank, iter.PRank())

	iter.Next()
	require.Equalf(t, true, iter.Done(), "iteration error - iterator not done")

}

func testItComputesPercentiles(t *testing.T) {
	h, _ := New(2)
	for _, i := range []uint{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		for j := uint(0); j < i; j++ {
			h.Update(i)
		}
	}
}
