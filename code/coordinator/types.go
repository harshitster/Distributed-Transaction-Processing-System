package coordinator

type TxnStatus string

const (
	TxnPending   TxnStatus = "PENDING"
	TxnPrepared  TxnStatus = "PREPARED"
	TxnCommitted TxnStatus = "COMMITTED"
	TxnAborted   TxnStatus = "ABORTED"
)

type Transaction struct {
	ID         string    `json:"id"`
	From       string    `json:"from"`
	To         string    `json:"to"`
	Amount     int       `json:"amount"`
	Status     TxnStatus `json:"status"`
	Timestamp  int64     `json:"timestamp"`
	ClientAddr string    `json:"client_addr"`
}
