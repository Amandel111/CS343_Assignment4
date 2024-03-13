package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

//type RaftNode int

type RaftNode struct {
	mutex           sync.Mutex
	selfID          int
	myPort          string
	currentTerm     int
	peerConnections []ServerConnection
	electionTimeout *time.Timer
	status          string
	votedFor        int
	voteCount       int
}

type VoteArguments struct {
	Term        int
	CandidateID int
}

type VoteReply struct {
	Term       int
	ResultVote bool
}

type AppendEntryArgument struct {
	Term     int
	LeaderID int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

type ServerConnection struct {
	serverID      int
	Address       string
	rpcConnection *rpc.Client
}

var selfID int
var serverNodes []ServerConnection
var currentTerm int
var votedFor int
var electionTimeout *time.Timer

const NUM_NODES = 5

// The RequestVote RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and majority votes
func (node *RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	fmt.Println(node.selfID, "recieved a vote request from", arguments.CandidateID)
	node.resetElectionTimeout()                                    //do we do this before or after we compare terms
	if arguments.Term >= node.currentTerm && node.votedFor == -1 { //reset votedFor at beginning of terms
		//candidate has valid term numver, approve vote
		fmt.Println("node votes for a candidate other than itself")
		reply.ResultVote = true
		node.votedFor = arguments.CandidateID
		fmt.Println("node ", node.selfID, " votes for node ", node.votedFor)

	} else { //ask christine if less than or equal to or just less than
		//candidate has equal to or smaller term number, invalid
		reply.ResultVote = false
	}

	//increment node term
	node.currentTerm += 1
	reply.Term = node.currentTerm
	return nil
}

// The AppendEntry RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and heartbeats
func (node *RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {
	//check if currently a candidate, if so and if you just received a valid heartbeat, return to follower state

	fmt.Println("append entries has been called, reset timer for node ", node.selfID)
	node.resetElectionTimeout() //do we do this before or after we compare term

	//receive heartbeat from true leader
	if arguments.Term >= node.currentTerm {
		fmt.Println("received append entry from valid leader")

		//reset who node has voted for because if receiving heartbeat, not in an election, this should be null
		node.votedFor = -1
		reply.Success = true
		if node.status == "leader" {
			//revert to follower, increment term
			node.status = "follower"
			node.currentTerm = arguments.Term
		}
	} else {
		//received an appendEntry from an old leader
		reply.Success = false
	}
	reply.Term = node.currentTerm
	return nil

}

// You may use this function to help with handling the election time out
// Hint: It may be helpful to call this method every time the node wants to start an election
func (node *RaftNode) LeaderElection() {
	fmt.Println("nominate self as candidate. node:", node.selfID)
	//increment current term and status
	node.currentTerm += 1
	node.status = "candidate"

	// vote for itself
	node.voteCount += 1
	node.votedFor = node.selfID

	// send election
	arguments := VoteArguments{
		Term:        node.currentTerm,
		CandidateID: node.selfID,
	}

	//wait groups
	var waitgroup sync.WaitGroup

	//potential term number to update our candidate node if it is behind
	updateTerm := node.currentTerm
	//fmt.Println("peer connections ", node.peerConnections, " for node ", node.selfID)
	for _, peerNode := range node.peerConnections {
		fmt.Println("requesting from node", peerNode)
		waitgroup.Add(1)
		go func(server ServerConnection) {
			defer waitgroup.Done()
			var reply VoteReply
			//var err string
			//var connectionError = "connection is shut down"
			err := server.rpcConnection.Call("RaftNode.RequestVote", arguments, &reply)
			//var currErr = err.Error()
			//if connectionError == currErr {
			// 	fmt.Println("true")
			// 	return
			// }
			for err != nil {
				err = server.rpcConnection.Call("RaftNode.RequestVote", arguments, &reply)
			}
			//fmt.Println("candidate ", node.selfID, " gets response: ", reply);
			if reply.ResultVote {
				//fmt.Print("node votes yes")
				node.voteCount += 1
			} else {
				if reply.Term > updateTerm {
					updateTerm = reply.Term
				}
			}
		}(peerNode)
	}
	waitgroup.Wait()

	fmt.Println("candidate ", node.selfID, " got ", node.voteCount, " votes")
	if float64(node.voteCount)/float64(NUM_NODES) > 0.5 {
		fmt.Println("confirmed leader")
		node.status = "leader"
		Heartbeat(node)
	} else {
		node.status = "follower"

		//update node term, updateTerm will be node.currentTerm unless node.currentTerm is out of date
		node.currentTerm = updateTerm
	}
	node.voteCount = 0

}

/* TODO:
reset votedFor
test with failures by making noes go to sleep randomly
*/

// You may use this function to help with handling the periodic heartbeats
// Hint: Use this only if the node is a leader
func Heartbeat(node *RaftNode) {
	fmt.Println("heartbeat called")

	//start a heartbeat timer
	node.electionTimeout = time.NewTimer(10 * time.Millisecond)

	go func() {
		for {
			//thread for each node checking for timeout
			<-node.electionTimeout.C

			// Printed when timer is fired
			//fmt.Println("heartbeat timer fired send heartbeat")

			//send heartbeat via appendentries
			arguments := AppendEntryArgument{
				Term:     node.currentTerm,
				LeaderID: node.selfID,
			}
			//var waitgroup sync.WaitGroup
			for _, peerNode := range node.peerConnections {
				//waitgroup.Add(1)
				/*go*/
				func(server ServerConnection) {
					//defer waitgroup.Done()
					var reply AppendEntryReply
					err := server.rpcConnection.Call("RaftNode.AppendEntry", arguments, &reply)
					if err != nil {
						//return
					}
					if !reply.Success {
						//update old leader's term
						node.currentTerm = reply.Term
						node.status = "follower"
						return
					}
					//fmt.Print("reply from append entry: ", reply);
				}(peerNode)
			}
			//waitgroup.Wait()

			//start a heartbeat timer
			node.electionTimeout = time.NewTimer(10 * time.Millisecond)
		}
	}()

}

// will initiate a timer for the node passed to it
func StartTimer(node *RaftNode) {
	//node.mutex.Lock() //dont need to protect because it will be reset every time a node reaches out to it
	//defer node.mutex.Unlock()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tRandom := time.Duration(r.Intn(150)+151) * time.Millisecond
	node.electionTimeout = time.NewTimer(tRandom)
	//fmt.Println("Timer started")
}

// resetElectionTimeout resets the election timeout to a new random duration.
// This function should be called whenever an event occurs that prevents the need for a new election,
// such as receiving a heartbeat from the leader or granting a vote to a candidate.
func (node *RaftNode) resetElectionTimeout() {
	fmt.Println("node ", node.selfID, " reset its timer")
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	duration := time.Duration(r.Intn(150)+151) * time.Millisecond
	node.electionTimeout.Stop()          // Use Reset method only on stopped or expired timers
	node.electionTimeout.Reset(duration) // Resets the timer to new random value
}

func main() {
	// The assumption here is that the command line arguments will contain:
	// This server's ID (zero-based), location and name of the cluster configuration file
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// Read the values sent in the command line

	// Get this sever's ID (same as its index for simplicity)
	myID, err := strconv.Atoi(arguments[1])
	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(arguments[2])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	node := &RaftNode{
		selfID:      myID,
		mutex:       sync.Mutex{},
		currentTerm: 0,
		status:      "follower",
		votedFor:    -1,
	}

	myPort := "localhost"

	// Read the IP:port info from the cluster configuration file
	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		log.Printf(text, index)
		if index == myID {
			myPort = text
			node.myPort = myPort
			index++
			continue
		}
		// Save that information as a string for now
		lines = append(lines, text)
		index++
	}
	// If anything wrong happens with readin the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Following lines are to register the RPCs of this object of type RaftNode
	//api := new(RaftNode)
	//err = rpc.Register(api)
	err = rpc.Register(node)
	if err != nil {
		log.Fatal("error registering the RPCs", err)
	}
	rpc.HandleHTTP()
	go http.ListenAndServe(node.myPort, nil)
	log.Printf("serving rpc on port" + node.myPort)

	// This is a workaround to slow things down until all servers are up and running
	// Idea: wait for user input to indicate that all servers are ready for connections
	// Pros: Guaranteed that all other servers are already alive
	// Cons: Non-realistic work around

	// reader := bufio.NewReader(os.Stdin)
	// fmt.Print("Type anything when ready to connect >> ")
	// text, _ := reader.ReadString('\n')
	// fmt.Println(text)

	// Idea 2: keep trying to connect to other servers even if failure is encountered
	// For fault tolerance, each node will continuously try to connect to other nodes
	// This loop will stop when all servers are connected
	// Pro: Realistic setup
	// Con: If one server is not set up correctly, the rest of the system will halt

	for index, element := range lines {
		// Attemp to connect to the other server node
		client, err := rpc.DialHTTP("tcp", element)
		// If connection is not established
		for err != nil {
			// Record it in log
			log.Println("Trying again. Connection error: ", err)
			// Try again!
			client, err = rpc.DialHTTP("tcp", element)
		}
		// Once connection is finally established
		// Save that connection information in the servers list
		serverNodes = append(serverNodes, ServerConnection{index, element, client})
		//fmt.Println("serverNodes:", serverNodes)
		// Record that in log
		fmt.Println("Connected to " + element)
	}
	node.peerConnections = serverNodes

	// Once all the connections are established, we can start the typical operations within Raft
	// Leader election and heartbeats are concurrent and non-stop in Raft

	// HINT 1: You may need to start a thread here (or more, based on your logic)
	// Hint 2: Main process should never stop
	// Hint 3: After this point, the threads should take over
	// Heads up: they never will be done!
	// Hint 4: wg.Wait() might be helpful here

	StartTimer(node)

	go func() {
		//thread for each node checking for timeout
		<-node.electionTimeout.C

		// Printed when timer is fired
		fmt.Println("timer inactivated for node", node.selfID)

		//if node reaches this point, it starts an election because it has not received a heartbeat
		node.LeaderElection()
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait() // Waits forever, so main process does not stop

}
