package mr

type JobChan struct {
	job *Job
	chn chan struct{}
}

func (c *JobChan) Release() {
	c.chn <- struct{}{}
}

func (c *JobChan) Done() chan struct{} {
	return c.chn
}

func BuildJobChan(job *Job) *JobChan {
	return &JobChan{
		job: job,
		chn: make(chan struct{}, 1),
	}
}
