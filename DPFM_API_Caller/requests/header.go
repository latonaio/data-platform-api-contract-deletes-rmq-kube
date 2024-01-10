package requests

type Header struct {
	Contract              	 int     `json:"Contract"`
	IsMarkedForDeletion      *bool   `json:"IsMarkedForDeletion"`
}
