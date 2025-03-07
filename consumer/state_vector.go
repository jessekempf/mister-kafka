package consumer

type stateVector struct {
	GroupID         string
	GenerationID    int
	MemberID        string
	GroupInstanceID string
	ProtocolType    string
	ProtocolName    string
}
