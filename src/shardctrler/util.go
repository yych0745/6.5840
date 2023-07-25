package shardctrler
type Queue struct {
	V []int64
}

func (q *Queue) init() {
	q.V = make([]int64, 0)
}

func (q *Queue) pop_front() int64 {
	if len(q.V) == 0 {
		panic("queeue 长度为0 无法pop")
	}
	res := q.V[0]
	q.V = q.V[1:]
	return res
}

func (q *Queue) push_back(u int64) {
	for len(q.V) >= 20 {
		q.pop_front()
	}
	q.V = append(q.V, u)
}