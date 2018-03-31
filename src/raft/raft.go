package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"math/rand"
	"time"
	"strconv"
	"fmt"
	"bytes"
	"encoding/gob"
)

// import "bytes"
// import "encoding/gob"

const FOLLOWER = 1
const CANDIDATE = 2
const LEADER = 3

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

func (obj ApplyMsg) String() string {
	return "ApplyMsg: Index - " + strconv.Itoa(obj.Index) + ", UseSnapshot - " + strconv.FormatBool(obj.UseSnapshot)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	log         []LogInfo

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	currentState int // 1 - Follower, 2 - Candidate, 3 - Leader
	votesGained  int

	appendEntriesChan chan ApplyMsg
	heartBeatChan     chan bool
	leaderElectChan   chan bool
	pollVoteChan      chan bool
	commitLogChan     chan bool
}

func (obj Raft) String() string {
	return "Raft: ID - " + strconv.Itoa(obj.me) + ", CurrentTerm - " + strconv.Itoa(obj.currentTerm) + ", VotedFor - " + strconv.Itoa(obj.votedFor) + "LogInfo Length - " + strconv.Itoa(len(obj.log)) + ", LogInfo - " + obj.log[len(obj.log)-1].String() + ", CommitIndex - " + strconv.Itoa(obj.commitIndex) + ", LastApplied - " + strconv.Itoa(obj.lastApplied) + ", CurrentState - " + strconv.Itoa(obj.currentState) + ", VotesGained - " + strconv.Itoa(obj.votesGained)
}

type LogInfo struct {
	LogIndex         int
	LogTerm          int
	CommandToExecute interface{}
}

func (obj LogInfo) String() string {
	cmd := -1
	if obj.CommandToExecute != nil {
		cmd = obj.CommandToExecute.(int)
	}
	return "LogInfo: LogIndex - " + strconv.Itoa(obj.LogIndex) + ", LogTerm - " + strconv.Itoa(obj.LogTerm) + ", Command - " + strconv.Itoa(cmd)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogInfo
	LeaderCommit int
}

func (obj AppendEntriesArgs) String() string {
	return "AppendEntriesArgs: Term - " + strconv.Itoa(obj.Term) + ", LeaderID - " + strconv.Itoa(obj.LeaderId) + ", PrevLogIndex - " + strconv.Itoa(obj.PrevLogIndex) + ", PrevLogTerm - " + strconv.Itoa(obj.PrevLogTerm) + ", LeaderCommit - " + strconv.Itoa(obj.LeaderCommit) + ", Entries - " + strconv.Itoa(len(obj.Entries))
}

type AppendEntriesReply struct {
	Term          int
	ExpectedIndex int
	Success       bool
}

func (obj AppendEntriesReply) String() string {
	return "AppendEntriesReply: Term - " + strconv.Itoa(obj.Term) + "ExpectedIndex - " + strconv.Itoa(obj.ExpectedIndex) + ", Success - " + strconv.FormatBool(obj.Success)
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	//printLog("Append Entries --- " + rf.String() + " --- " + args.String())
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ExpectedIndex = rf.getLastLog().LogIndex + 1
		//printLog("Append Entries --- " + reply.String())
		return
	} else {
		go func() {
			rf.heartBeatChan <- true
		}()

		if args.Term > rf.currentTerm {
			rf.currentState = FOLLOWER
			rf.currentTerm = args.Term
			rf.votedFor = -1

			reply.Success = false
		}

		reply.Term = args.Term
		//printLog("Append Entries --- " + reply.String() + " --- " + rf.String())

		lastLogIndex := rf.getLastLog().LogIndex
		if args.PrevLogIndex > lastLogIndex {
			reply.Success = false
			reply.ExpectedIndex = lastLogIndex + 1
			//printLog("Lagging: " + rf.String() + " ;; " + args.String() + " ;; " + reply.String())
			return
		}

		firstLogIndex := rf.log[0].LogIndex

		if firstLogIndex < args.PrevLogIndex {
			termOfPrevIndex := rf.log[args.PrevLogIndex-firstLogIndex].LogTerm

			if termOfPrevIndex != args.PrevLogTerm {
				for i := args.PrevLogIndex - 1; i >= firstLogIndex; i-- {
					if rf.log[i-firstLogIndex].LogTerm != termOfPrevIndex {
						reply.ExpectedIndex = i + 1
						//printLog("OutOfSync: " + rf.String() + " ;; " + args.String() + " ;; " + reply.String())
						break
					}
				}
				return
			}
		}

		if firstLogIndex <= args.PrevLogIndex {
			rf.log = rf.log[:args.PrevLogIndex-firstLogIndex+1]

			if args.Entries != nil {
				rf.log = append(rf.log, args.Entries...)

				if args.LeaderCommit > rf.commitIndex {
					rf.checkAndUpdateCommitIndex(args.LeaderCommit)
				}
			} else {
				if args.PrevLogTerm == rf.log[args.PrevLogIndex-firstLogIndex].LogTerm {
					rf.checkAndUpdateCommitIndex(args.LeaderCommit)
				}
			}

			reply.Success = true
			rf.currentTerm = args.Term
			reply.ExpectedIndex = rf.getLastLog().LogIndex + 1

			//printLog("AppendSuccess: " + rf.String() + " ;; " + args.String() + " ;; " + reply.String())
		}
	}

	return
}

func (rf *Raft) checkAndUpdateCommitIndex(leaderCommitIndex int) {
	lastLogIndex := rf.getLastLog().LogIndex
	rf.commitIndex = leaderCommitIndex
	if lastLogIndex < leaderCommitIndex {
		rf.commitIndex = lastLogIndex
	}

	//printLog("CommitOrder Received: " + rf.String() + " ;; " + strconv.Itoa(leaderCommitIndex))
	go func() {
		rf.commitLogChan <- true
	}()
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		//printLog("Received reply for heartbeat from #" + strconv.Itoa(server) + " ;; " + rf.String() + " --- " + args.String() + " --- " + reply.String())

		if rf.currentState == LEADER && rf.currentTerm == args.Term {
			if reply.Success {
				if len(args.Entries) > 0 {
					//printLog("Updating Leader on successful heartbeat: Server - " + strconv.Itoa(server) + ", Reply - " + reply.String())
					rf.nextIndex[server] = reply.ExpectedIndex
					rf.matchIndex[server] = reply.ExpectedIndex - 1
				}
			} else if reply.Term > rf.currentTerm {
				//printLog("Leader #" + strconv.Itoa(rf.me) + " not in sync. Switching to follower.")
				rf.currentState = FOLLOWER
				rf.currentTerm = reply.Term
				rf.votedFor = -1

				rf.persist()
			} else {
				//printLog("Some Failure: " + strconv.Itoa(server) + ", Reply - " + reply.String())
				rf.nextIndex[server] = reply.ExpectedIndex
			}
		}
	}

	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.currentState == LEADER
	rf.mu.Unlock()

	//printLog("State Checking: #" + strconv.Itoa(rf.me) + ", Term - " + strconv.Itoa(term) + ", Is Leader - " + strconv.FormatBool(isleader) + ", State - " + strconv.Itoa(rf.currentState))
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

func (obj RequestVoteArgs) String() string {
	return "RequestVoteArgs: Term - " + strconv.Itoa(obj.Term) + ", CandidateID - " + strconv.Itoa(obj.CandidateID) + ", LastLogIndex - " + strconv.Itoa(obj.LastLogIndex) + ", LastLogTerm - " + strconv.Itoa(obj.LastLogTerm)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (obj RequestVoteReply) String() string {
	return "RequestVoteReply: Term - " + strconv.Itoa(obj.Term) + ", VoteGranted - " + strconv.FormatBool(obj.VoteGranted)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	//printLog("Requesting Vote --- " + rf.String() + " --- " + args.String())

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.currentState = FOLLOWER
			rf.votedFor = -1
		}

		reply.Term = rf.currentTerm
		lastLog := rf.getLastLog()

		if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && (lastLog.LogTerm < args.LastLogTerm || (lastLog.LogTerm == args.LastLogTerm && lastLog.LogIndex <= args.LastLogIndex)) {
			go func() {
				rf.pollVoteChan <- true
			}()

			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	}
	//printLog(rf.String() + " --- " + args.String() + " --- " + reply.String())
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		//printLog("Received reply from #" + strconv.Itoa(server) + "; " + rf.String() + " --- " + args.String() + " --- " + reply.String())

		if rf.currentState != CANDIDATE {
			return ok
		}

		if reply.VoteGranted {
			rf.votesGained++
			if rf.votesGained > len(rf.peers)/2 {
				go func() {
					rf.leaderElectChan <- true
				}()
			}
		} else if reply.Term > rf.currentTerm {
			rf.currentState = FOLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1

			rf.persist()
		}
	}

	return ok
}

func (rf *Raft) initiateElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateID = rf.me

	lastLog := rf.getLastLog()
	args.LastLogIndex = lastLog.LogIndex
	args.LastLogTerm = lastLog.LogTerm

	//printLog(rf.String() + " --- " + args.String())
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			//printLog(strconv.Itoa(rf.me) + " sending request vote to #" + strconv.Itoa(i))
			go rf.sendRequestVote(i, &args, &RequestVoteReply{})
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.currentState == LEADER

	if isLeader {
		index = rf.getLastLog().LogIndex + 1
		term = rf.currentTerm
		rf.log = append(rf.log, LogInfo{index, term, command})

		rf.persist()
	}

	//printLog(strconv.Itoa(rf.me) + " - " + strconv.Itoa(index) + " - " + strconv.Itoa(term) + " - " + strconv.FormatBool(isLeader))

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.pollVoteChan = make(chan bool)
	rf.leaderElectChan = make(chan bool)
	rf.heartBeatChan = make(chan bool)
	rf.commitLogChan = make(chan bool)
	rf.appendEntriesChan = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.currentState = 1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votesGained = 0
	rf.log = append(rf.log, LogInfo{LogIndex: 0, LogTerm: 0})
	rf.readPersist(persister.ReadRaftState())

	//printLog(rf.String())

	go func() {
		for {
			switch rf.currentState {
			case FOLLOWER:
				rf.doFollowerJob()
			case CANDIDATE:
				rf.doCandidateJob()
			case LEADER:
				rf.doLeaderJob()
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	go func() {
		for {
			<-rf.commitLogChan
			//printLog("Committing: " + rf.String())
			rf.mu.Lock()
			firstLogIndex := rf.log[0].LogIndex

			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				cmdToCommit := rf.log[i-firstLogIndex].CommandToExecute
				msg := ApplyMsg{Index: i, Command: cmdToCommit}
				//cmdInt := cmdToCommit.(int)
				//printLog("Committing: Raft ID - " + strconv.Itoa(rf.me) + ", Index - " + strconv.Itoa(i) + ", Command - " + strconv.Itoa(cmdInt))
				applyCh <- msg
				rf.lastApplied = i
			}
			rf.mu.Unlock()
		}
	}()

	return rf
}

func (rf *Raft) doFollowerJob() {
	select {
	case <-rf.heartBeatChan:
		//printLog("Received heart beat --- " + rf.String())
	case <-rf.pollVoteChan:
		//printLog(strconv.Itoa(rf.me) + " voted for #" + strconv.Itoa(rf.votedFor))
	case <-time.After(time.Millisecond * time.Duration(rf.getTimeoutInMs())):
		//printLog("Switching to candidate --- " + rf.String())
		rf.currentState = CANDIDATE
	}
}

func (rf *Raft) doCandidateJob() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votesGained = 1
	rf.mu.Unlock()
	rf.persist()

	go rf.initiateElection()

	select {
	case <-time.After(time.Millisecond * time.Duration(rf.getTimeoutInMs())):
		//printLog("Failed to gain enough votes before the deadline. Repeating the task again. #" + strconv.Itoa(rf.me))
	case <-rf.heartBeatChan:
		//printLog("Received HeartBeat. Switching to Follower --- " + rf.String())
		rf.mu.Lock()
		rf.currentState = FOLLOWER
		rf.mu.Unlock()
	case <-rf.leaderElectChan:
		//printLog("Got max votes. Switching to Leader --- " + rf.String())
		rf.mu.Lock()
		rf.currentState = LEADER
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))

		lastLogIndex := rf.getLastLog().LogIndex
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = lastLogIndex + 1
			//printLog("NextIndexCheck: " + strconv.Itoa(rf.nextIndex[i]))
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) doLeaderJob() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	firstLogIndex := rf.log[0].LogIndex
	lastLogIndex := rf.getLastLog().LogIndex
	N := rf.commitIndex;
	//printLog("Sending RPC: " + rf.String())
	//printLog("Sending RPC: LastLogIndex - " + strconv.Itoa(lastLogIndex))
	for N = lastLogIndex; N > rf.commitIndex; N-- {
		noOfPeersInSync := 1
		for j := 0; j < len(rf.peers); j++ {
			if j != rf.me && rf.matchIndex[j] >= N && rf.log[N].LogTerm == rf.currentTerm {
				noOfPeersInSync++
			}
		}

		if noOfPeersInSync > len(rf.peers)/2 {
			break;
		}
	}

	//printLog("Commiting before RPC: N - " + strconv.Itoa(N) + ", RF - " + rf.String())
	if N > rf.commitIndex {
		rf.commitIndex = N
		go func() {
			rf.commitLogChan <- true
		}()
	}

	for i := 0; i < len(rf.peers) && rf.currentState == LEADER; i++ {
		//printLog(strconv.Itoa(lastLog.LogIndex) + " -- " + strconv.Itoa(rf.nextIndex[i]) + " -- " + strconv.FormatBool(lastLog.LogIndex >= rf.nextIndex[i]))
		if i != rf.me {
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex-firstLogIndex].LogTerm

			args.Entries = make([]LogInfo, lastLogIndex-args.PrevLogIndex)
			copy(args.Entries, rf.log[args.PrevLogIndex+1-firstLogIndex:])

			//printLog(rf.String() + " --- " + args.String())
			//printLog("Calling AppendEntries for #" + strconv.Itoa(i))

			go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) getLastLog() LogInfo {
	lastLog := rf.log[len(rf.log)-1]
	return lastLog
}

func (rf *Raft) getTimeoutInMs() int {
	rand.Seed(time.Now().UTC().UnixNano())
	timeout := rand.Intn(200) + 200
	//printLog("Timeout of server #" + strconv.Itoa(rf.me) + " is " + strconv.Itoa(timeout))
	return timeout
}

func PrintLog(content string) {
	fmt.Println(content)
	fmt.Println()
}
