package lsm

type CompactionStatus int

const (
	CompactionUndo CompactionStatus = iota
	CompactionFinish
	CompactionProcessing
)

type Compactor struct {
	waitList chan *MemTable

	status map[uint64]CompactionStatus
}

func NewCompactor() *Compactor {
	return &Compactor{
		waitList: make(chan *MemTable),
		status:   make(map[uint64]CompactionStatus),
	}
}

func (c *Compactor) CheckStatus(fileNumber uint64) CompactionStatus {
	return c.status[fileNumber]
}

func (c *Compactor) Run() {
	for {
		table := <-c.waitList
		c.compaction(table)
	}
}

func (c *Compactor) compaction(table *MemTable) {

}
