package coordinator

type TxnStatus string

const (
	TxnPending   TxnStatus = "PENDING"
	TxnPrepared  TxnStatus = "PREPARED"
	TxnCommitted TxnStatus = "COMMITTED"
	TxnAborted   TxnStatus = "ABORTED"
)

type Transaction struct {
	ID        string    `json:"id"`
	Accounts  []string  `json:"accounts"`
	Op        string    `json:"op"` // add, sub, transfer, get
	Amount    int       `json:"amount"`
	Status    TxnStatus `json:"status"`
	Timestamp int64     `json:"timestamp"`
}
