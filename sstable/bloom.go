package sstable

type bloomFilterGenerator struct {
	k       uint8
	hashKey []uint32
}

func NewBloomFilterGenerator() *bloomFilterGenerator {
	return &bloomFilterGenerator{
		k:       7, // ln2 * m / n = ln2 * 10
		hashKey: make([]uint32, 0),
	}
}

func (f *bloomFilterGenerator) add(key []byte) {
	f.hashKey = append(f.hashKey, hash(key))
}

func (f *bloomFilterGenerator) build() []byte {
	numBits := len(f.hashKey) * int(f.k)
	numBytes := (numBits + 7) / 8
	numBits = numBytes * 8

	bitsList := make([]byte, numBytes+1)
	bitsList[numBytes] = f.k
	for _, key := range f.hashKey {
		delta := (key >> 17) | (key << 15)
		for j := uint8(0); j < f.k; j++ {
			bitpos := key % uint32(numBits)
			bitsList[bitpos/8] |= (1 << (bitpos % 8))
			key += delta
		}
	}
	f.hashKey = f.hashKey[:0]
	return bitsList
}

type bloomFilter struct{}

func (b *bloomFilter) contain(filter, key []byte) bool {
	nBytes := len(filter) - 1
	if nBytes < 1 {
		return false
	}
	nBits := uint32(nBytes * 8)

	k := filter[nBytes]

	kh := hash(key)
	delta := (kh >> 17) | (kh << 15)
	for j := uint8(0); j < k; j++ {
		bitpos := kh % nBits
		if (uint32(filter[bitpos/8]) & (1 << (bitpos % 8))) == 0 {
			return false
		}
		kh += delta
	}
	return true
}
