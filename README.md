package main

import ( context","encoding/json", "flag", "fmt", "log", "net/http", "sync","time"}

}

// Job struct to represent a task
type Job struct {
 ID        string    json:"id"
 Task      string    json:"task"
 Arguments map[string]interface{} json:"arguments"
 Status    string    json:"status"
 Result    string    json:"result"
 CreatedAt time.Time json:"created_at"
 UpdatedAt time.Time json:"updated_at"
}

// JobStatus enum for tracking job status
type JobStatus string

const (
 Pending  JobStatus = "PENDING"
 Running  JobStatus = "RUNNING"
 Completed JobStatus = "COMPLETED"
 Failed   JobStatus = "FAILED"
)

// Configuration for the job scheduler
type Config struct {
 RedisAddr  string
 RedisDB    int
 NumWorkers int
}

// Redis client instance
var redisClient *redis.Client

// Global job queue
var jobQueue chan *Job

// Initialize Redis connection
func initRedis(addr string, db int) *redis.Client {
 client := redis.NewClient(&redis.Options{
  Addr:     addr,
  Password: "", // No password for this example
  DB:       db,
 })

 if _, err := client.Ping(context.Background()).Result(); err != nil {
  log.Fatal("Error connecting to Redis:", err)
 }
 return client
}

// Function to add a new job to the queue
func addJob(job *Job) error {
 data, err := json.Marshal(job)
 if err != nil {
  return err
 }
 return redisClient.RPush(context.Background(), "job_queue", data).Err()
}

// Function to get a job from the queue
func getJob() (*Job, error) {
 result, err := redisClient.LPop(context.Background(), "job_queue").Result()
 if err == redis.Nil {
  return nil, nil // Queue is empty
 }
 if err != nil {
  return nil, err
 }
 job := &Job{}
 err = json.Unmarshal([]byte(result), job)
 return job, err
}

// Function to execute a job
func executeJob(job *Job) {
 job.Status = Running.String()
 // Update the job status in Redis
 updateJob(job)

 // Simulate job execution
 time.Sleep(time.Duration(rand.Intn(5)+1) * time.Second)

 job.Status = Completed.String()
 // Update the job status in Redis
 updateJob(job)
 // Generate a random result (for demonstration)
 job.Result = fmt.Sprintf("Job %s completed successfully.", job.ID)
}

// Function to update a job in Redis
func updateJob(job *Job) error {
 job.UpdatedAt = time.Now()
 data, err := json.Marshal(job)
 if err != nil {
  return err
 }
 return redisClient.HSet(context.Background(), job.ID, "job", data).Err()



}

// Function to get a job by ID from Redis
func getJobByID(id string) (*Job, error) {
 result, err := redisClient.HGet(context.Background(), id, "job").Result()
 if err == redis.Nil {
  return nil, fmt.Errorf("job not found: %s", id)
 }
 if err != nil {
  return nil, err
 }
 job := &Job{}
 err = json.Unmarshal([]byte(result), job)
 return job, err
}

// Function to handle job scheduling requests
func handleJobScheduling(w http.ResponseWriter, r *http.Request) {
 if r.Method == http.MethodPost {
  // Decode the request body
  decoder := json.NewDecoder(r.Body)
  var job Job
  err := decoder.Decode(&job)
  if err != nil {
   http.Error(w, "Invalid request body", http.StatusBadRequest)
   return
  }

  // Generate a unique ID for the job
  job.ID = uuid.New().String()
  job.Status = Pending.String()
  job.CreatedAt = time.Now()
  job.UpdatedAt = time.Now()

  // Add the job to the queue
  err = addJob(&job)
  if err != nil {
   http.Error(w, "Error adding job to queue", http.StatusInternalServerError)
   return
  }

  // Respond with the job ID
  fmt.Fprintf(w, "Job ID: %sn", job.ID)
 } else {
  // Display a form to input the job details
  fmt.Fprintln(w, "Enter job details:")
  fmt.Fprintln(w, "<form method='POST' action='/jobs'>")
  fmt.Fprintln(w, "<label for='task'>Task:</label>")
  fmt.Fprintln(w, "<input type='text' id='task' name='task' required />")
  fmt.Fprintln(w, "<label for='arguments'>Arguments (JSON):</label>")
  fmt.Fprintln(w, "<textarea id='arguments' name='arguments'></textarea>")
  fmt.Fprintln(w, "<input type='submit' value='Schedule' />")
  fmt.Fprintln(w, "</form>")
 }
}

// Function to handle job status requests
func handleJobStatus(w http.ResponseWriter, r *http.Request) {
 vars := mux.Vars(r)
 jobID := vars["jobID"]

 // Get the job from Redis
 job, err := getJobByID(jobID)
 if err != nil {
  http.Error(w, "Job not found", http.StatusNotFound)
  return
 }

 // Encode the job as JSON and respond
 data, err := json.Marshal(job)
 if err != nil {
  http.Error(w, "Error encoding job", http.StatusInternalServerError)
  return
 }
 w.WriteHeader(http.StatusOK)
 w.Write(data)
}

func main() {
 // Parse command-line flags
 redisAddr := flag.String("redis-addr", "localhost:6379", "Redis address")
 redisDB := flag.Int("redis-db", 0, "Redis database")
 numWorkers := flag.Int("workers", 4, "Number of worker goroutines")
 flag.Parse()

 // Initialize Redis connection
 redisClient = initRedis(*redisAddr, *redisDB)

 // Create job queue channel
 jobQueue = make(chan *Job, *numWorkers)

 // Start worker goroutines
 var wg sync.WaitGroup
 wg.Add(*numWorkers)
 for i := 0; i < *numWorkers; i++ {
  go func() {
   defer wg.Done()
   for job := range jobQueue {
    executeJob(job)
   }
  }
 }

 // Create a new router
 router := mux.NewRouter()

 // Route for job scheduling
 router.HandleFunc("/jobs", handleJobScheduling).Methods("GET", "POST")

 // Route for job status
 router.HandleFunc("/jobs/{jobID}", handleJobStatus).Methods("GET")

 // Start the server
 fmt.Println("Server listening on port 8080")
 log.Fatal(http.ListenAndServe(":8080", router))
}
