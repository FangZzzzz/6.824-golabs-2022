package mr

type JobQueue struct {
	jobs []*Job
}

func (q *JobQueue) Push(job *Job) {
	q.jobs = append(q.jobs, job)
}

func (q *JobQueue) Pop() (*Job, bool) {
	if len(q.jobs) == 0 {
		return nil, false
	}

	job := q.jobs[len(q.jobs)-1]
	q.jobs = q.jobs[:len(q.jobs)-1]
	return job, true
}

func (q *JobQueue) Empty() bool {
	return len(q.jobs) == 0
}
