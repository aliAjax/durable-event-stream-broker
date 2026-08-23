package domain

type ReplicaState string

const (
	ReplicaFollower ReplicaState = "follower"
	ReplicaLeader   ReplicaState = "leader"
	ReplicaOffline  ReplicaState = "offline"
)

type Replica struct {
	ID         string
	State      ReplicaState
	Epoch      Epoch
	ISR        bool
	LastOffset Offset
}
type ReplicaSet struct {
	TopicID   string
	Partition int
	Leader    string
	Epoch     Epoch
	Replicas  map[string]*Replica
}

func NewReplicaSet(topic string, part int, ids []string) *ReplicaSet {
	rs := &ReplicaSet{TopicID: topic, Partition: part, Epoch: 1, Replicas: map[string]*Replica{}}
	for i, id := range ids {
		s := ReplicaFollower
		if i == 0 {
			s = ReplicaLeader
			rs.Leader = id
		}
		rs.Replicas[id] = &Replica{ID: id, State: s, Epoch: 1, ISR: true}
	}
	return rs
}
func (r *ReplicaSet) Failover(candidate string) error {
	c, ok := r.Replicas[candidate]
	if !ok || !c.ISR || c.State == ReplicaOffline {
		return E(ErrUnavailable, "candidate not in ISR")
	}
	r.Epoch++
	for _, v := range r.Replicas {
		if v.State == ReplicaLeader && v.ID == candidate {
			v.State = ReplicaFollower
		}
		v.Epoch = r.Epoch
	}
	c.State = ReplicaLeader
	r.Leader = candidate
	return nil
}
func (r *ReplicaSet) Reconcile(offsets map[string]Offset) {
	for id, o := range offsets {
		if v, ok := r.Replicas[id]; ok {
			v.LastOffset = o
			v.ISR = o >= 0
		}
	}
}
