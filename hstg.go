package hstg

import (
	"fmt"
	"math"
)

// binCodec is an interface used by an histogram
// to map input/output values to and from their
// appropriate bins
type binCodec interface {
	encode(value uint) uint
	decode(binValue uint) uint
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
//
// defaultCodec
//
///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

// defaultCodec is used to map values to bins of a given width
type defaultCodec struct {
	binWidth uint
}

// newDefaultCodec returns a valid new default codec
func newDefaultCodec(binWidth uint) (binCodec, error) {
	if binWidth == 0 {
		return nil, fmt.Errorf("invalid bin width: %d", binWidth)
	}
	return &defaultCodec{binWidth}, nil
}

// encode is a binner function
func (d *defaultCodec) encode(value uint) uint {
	return value / d.binWidth
}

// decode is a binner function
func (d *defaultCodec) decode(binValue uint) uint {
	return binValue * d.binWidth
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
//
// expCodec
//
///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

// ExpCodec is a codec for creating histogram of exponential data
type expCodec struct {
	logBase uint
}

// newExpCodec return as a valid exponential codec
func newExpCodec(logBase uint) (binCodec, error) {
	if logBase < 2 {
		return nil, fmt.Errorf("invalid log base for expCodec: %d", logBase)
	}
	return &expCodec{logBase}, nil
}

// encode is a binner function
func (e *expCodec) encode(value uint) uint {
	valueF, logBaseF := float64(value), float64(e.logBase)
	resultF := math.Log1p(valueF) / math.Log(logBaseF)
	return uint(resultF)
}

// Decode is a binner function
func (e *expCodec) decode(binValue uint) uint {
	binValueF, logBaseF := float64(binValue), float64(e.logBase)
	result := math.Pow(logBaseF, binValueF)
	return uint(result)
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
//
// hBin
//
///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

// hBin holds the bin size and its frequency. Also points to the next bin,
// so ordered insertion can be O(1)
type hBin struct {
	value uint
	freq  uint
	next  *hBin
}

func (b *hBin) update(freq uint) {
	b.freq += freq
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
//
// hBinList
//
///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

// hBinList holds a list of hBins
type hBinList struct {
	length    uint
	totalFreq uint
	head      *hBin
	curr      *hBin
}

func newBinList() *hBinList {
	return &hBinList{}
}

func (l *hBinList) iter(codec binCodec) *BinIter {
	return newIter(l, codec)
}

func (l *hBinList) update(value, freq uint) {

	if l.head == nil && l.curr != nil {
		panic("current node of an empty list is non nil")

	} else if l.curr == nil {
		// always start from the head if there is not a current element defined
		// even when the list is empty
		bin := l.binFor(&l.head, value)
		l.curr = bin // keep it
		bin.update(freq)

	} else if value < l.curr.value {
		// when the value to be updated is less than the current element,
		// we are passed its insertion point and we have to go back from the start
		bin := l.binFor(&l.head, value)
		l.curr = bin // keep it
		bin.update(freq)

	} else if value >= l.curr.value {
		// if the update value is greater than or equal to the current element's value
		// we pick up the search from the current element in order to ammortize the cost
		// of the operation
		bin := l.binFor(&l.curr, value)
		l.curr = bin // keep it
		bin.update(freq)

	} else {
		panic("sentinel error: this else should not be reached")
	}
	l.totalFreq += freq
}

func (l *hBinList) binFor(head **hBin, value uint) *hBin {
	var curr *hBin
	for {
		// either an empty list or the tail of a non-empty list
		curr = *head
		if curr == nil {
			result := &hBin{value, 0, nil}
			*head = result
			l.length++
			return result
		}
		if curr.value == value { // there is already an element with the given value
			return curr
		} else if curr.value < value { // haven't found the spot yet
			head = &curr.next
			continue
		}
		// we passed the spot. Go back one
		break
	}
	result := &hBin{value, 0, curr}
	*head = result
	l.length++
	return result
}

func (l *hBinList) first() *hBin {
	return l.head
}

func (l *hBinList) last() *hBin {
	bin := l.head
	for bin != nil && bin.next != nil {
		bin = bin.next
	}
	return bin
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
//
// hBinIter
//
///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

// BinIter is an iterator over a hBinList
type BinIter struct {
	binList *hBinList
	curr    *hBin
	cumFreq uint
	codec   binCodec
}

func newIter(binList *hBinList, codec binCodec) *BinIter {
	return &BinIter{binList, binList.head, 0, codec}
}

func (i *BinIter) bin() *hBin {
	return i.curr
}

// Done indicates wheter the iteration has finished
func (i *BinIter) Done() bool {
	return i.curr == nil
}

// Next positions the iterator on the next bin
func (i *BinIter) Next() {
	i.cumFreq += i.curr.freq
	i.curr = i.curr.next
}

// Freq returns the bin frequency
func (i *BinIter) Freq() uint {
	return i.curr.freq
}

// PRank returns the percentile rank for the current bin
func (i *BinIter) PRank() float64 {
	return (float64(i.cumFreq) / float64(i.binList.totalFreq)) * 100.0
}

// Percentile returns the percentile associated with the lower bound of the current bin
func (i *BinIter) Percentile() uint {
	return i.codec.decode(i.curr.value)
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
//
// Hstg
//
///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

// Hstg is used to represent an histogram
type Hstg struct {
	codec   binCodec
	binList *hBinList
}

// New returns a histogram ready for use
func New(binWidth uint) (*Hstg, error) {
	codec, err := newDefaultCodec(binWidth)
	if err != nil {
		return nil, err
	}
	return &Hstg{codec, newBinList()}, nil
}

// NewExp returns a histogram for exponential data ready for use
func NewExp(logBase uint) (*Hstg, error) {
	codec, err := newExpCodec(logBase)
	if err != nil {
		return nil, err
	}
	return &Hstg{codec, newBinList()}, nil
}

// BinCount returns the current number of bins in the histogram
func (h *Hstg) BinCount() uint {
	return h.binList.length
}

// TotalFreq returns the number of entries in the histogram
func (h *Hstg) TotalFreq() uint {
	return h.binList.totalFreq
}

// Update will find the right bin and update its frequency
// This operation is O(n) with ammortization for monotonically increasing values
func (h *Hstg) Update(value uint) {
	binValue := h.codec.encode(value)
	h.binList.update(binValue, 1)
}

// Percentile receives a float between 0. and 100.0 and it computes
// the percentile of the underlying grouped data
// An error is returned if the percentile is not within the range [0.0, 1.0].
// 0 is returned when the histogram is empty
// This operation is O(n) with no ammortization
func (h *Hstg) Percentile(prank float64) (uint, error) {
	var bin *hBin

	if prank < 0.0 || prank > 100.0 {
		return 0.0, fmt.Errorf("invalid prank: %v", prank)

	} else if h.binList.length == 0 {
		// 0 is the percentile of any prank in an empty histogram
		return h.codec.decode(0), nil

	} else if prank == 0.0 {
		// return the first element when prank == 0
		bin = h.binList.first()

	} else if prank == 100.0 {
		// return the last element when prank == 100
		bin = h.binList.last()

	} else {
		i := h.binList.iter(h.codec)
		bin = i.bin()
		for !i.Done() {
			if i.PRank() > prank {
				break
			}
			bin = i.bin()
			i.Next()
		}
	}

	result := h.codec.decode(bin.value)
	return result, nil
}

// Iter returns an iterator of bins to the caller
func (h *Hstg) Iter() *BinIter {
	return newIter(h.binList, h.codec)
}
