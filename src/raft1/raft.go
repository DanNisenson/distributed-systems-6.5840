package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	logmod "6.5840"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Role string

var (
	follower  Role = "follower"
	candidate Role = "candidate"
	leader    Role = "leader"
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	role        Role
	currentTerm int
	votedFor    int
	votes       *Votes

	abortHeartbeat context.CancelFunc
	abortElection  context.CancelFunc
	checkpoint     int64

	logger *logmod.Logger
}

// ------------------------------
// INIT

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.role = follower
	rf.votes = NewVotes(len(peers))
	rf.checkpoint = time.Now().UnixMilli()
	rf.votedFor = -1

	rf.logger = logmod.NewLogger()
	rf.logger.AddDefault("[ID %d] ", me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	return rf
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.pork("GetState", " term: %d role: %s", rf.currentTerm, rf.role)
	return rf.currentTerm, rf.role == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// ------------------------------
// RPC HANDLERS

type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// candidate is outdated
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.pork("RequestVote", "candidate: %d has term: %d < than mine: %d", args.CandidateId, args.Term, rf.currentTerm)
	} else if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		// already voted in this term's election
		reply.VoteGranted = false
		rf.pork("RequestVote", "I have already voted for server: %d in term: %d", rf.votedFor, rf.currentTerm)
	} else {
		// vote for candidate
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		rf.pork("RequestVote", "voting for server: %d in term: %d", rf.votedFor, rf.currentTerm)
	}
	rf.checkpoint = time.Now().UnixMilli()
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.pork("AppendEntries", "outdated leader: %d has term: %d < than mine: %d", args.LeaderId, args.Term, rf.currentTerm)
		reply.Success = false
		return
	} else {
		rf.pork("AppendEntries - Make follower", "new leader: %d has term: %d > than mine: %d", args.LeaderId, args.Term, rf.currentTerm)
		if args.Term == rf.currentTerm && rf.role == leader {
			rf.pork("ERROR 2 leaders found", "I'm leader but server: %d also is. term: %d", args.LeaderId, rf.currentTerm)
		}
		reply.Success = true
		rf.makeFollower(reply.Term)
	}
	rf.checkpoint = time.Now().UnixMilli()
}

// ------------------------------
// ROLE TRANSITIONS

func (rf *Raft) makeFollower(newTerm int) {
	rf.pork("Make follower")

	prevRole := rf.role
	rf.role = follower
	rf.currentTerm = newTerm

	switch prevRole {
	case candidate:
		rf.pork("Demoted from candidate to follower", "set abortElection")
		rf.abortElection()
	case leader:
		rf.pork("Demoted from leader to follower", "set abortHeartbeat")
		rf.abortHeartbeat()
	}
}

func (rf *Raft) makeCandidate() {
	rf.pork("Make candidate")

	rf.mu.Lock()
	if rf.role == leader {
		rf.pork("Demoted from leader to candidate", "set abortHeartbeat")
		rf.abortHeartbeat()
	}
	rf.role = candidate
	rf.currentTerm += 1
	rf.votes.Vote()
	rf.votedFor = rf.me
	rf.checkpoint = time.Now().UnixMilli()
	rf.mu.Unlock()

	rf.requestVotes()
}

func (rf *Raft) makeLeader() {
	rf.pork("Make leader")

	rf.role = leader
	if rf.role == candidate {
		rf.pork("Promoted from candidate to leader", "set abortElection")
		rf.abortElection()
	}
	go rf.startHeartbeat()
}

// ------------------------------
// ELECTIONS

func (rf *Raft) electionTicker() {
	for {
		if rf.shouldStartElection() {
			rf.makeCandidate()
		}

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) shouldStartElection() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	now := time.Now().UnixMilli()
	ms := 1000 + (rand.Int63() % 1500)

	return now-rf.checkpoint > ms && rf.role != leader
}

func (rf *Raft) requestVotes() {
	ctx, cancel := context.WithCancel(context.Background())
	rf.abortElection = cancel

	for i := range rf.peers {
		if i != rf.me {
			go rf.requestVoteFromServer(ctx, i)
		}
	}
}

func (rf *Raft) requestVoteFromServer(ctx context.Context, server int) {
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	reply := RequestVoteReply{}

	for loop := true; loop; {
		select {
		case <-ctx.Done():
			rf.pork("Abort election")
			return
		default:
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				rf.logger.Error("sendRequestVote failed for server: %d. Retry.", server)
				time.Sleep(100 * time.Millisecond)
			} else {
				rf.handleRequestVote(server, &reply)
				loop = false
			}
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) handleRequestVote(server int, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.pork("handleRequestVote", "term %d server: %d term > me", server, rf.currentTerm)
		rf.makeFollower(reply.Term)
	} else if reply.VoteGranted {
		rf.pork("handleRequestVote", "term %d server: %d voted for me", server, rf.currentTerm)
		rf.votes.Vote()
		if rf.votes.HasMajority() && rf.role == candidate {
			rf.makeLeader()
		}
	} else {
		rf.pork("handleRequestVote", "term %d server: %d didn't vote for me", server, rf.currentTerm)
	}
}

// ------------------------------
// HEARTBEAT

func (rf *Raft) startHeartbeat() {
	rf.pork("Start heartbeat")
	cycleCtx, cancel := context.WithCancel(context.Background())
	rf.abortHeartbeat = cancel

	for {
		rf.pork("Send heartbeat")
		select {
		case <-cycleCtx.Done():
			rf.pork("Abort heartbeat")
			return
		default:
			for i := range rf.peers {
				if i != rf.me {
					go func(i int) {
						rf.sendHeartbeat(i)
					}(i)
				}
			}
		}

		ms := 120
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeat(server int) {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(server, &args, &reply)
	if ok {
		rf.handleAppendEntries(&reply)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) handleAppendEntries(reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.makeFollower(reply.Term)
	}
}

// ------------------------------
// UTILS

// pritn logs to file & to porcupine
func (rf *Raft) pork(ev string, details ...any) {

	det := fmt.Sprintf("[%s]", time.Now().Format("15:04:05.000"))

	if len(details) > 0 {
		det += " " + fmt.Sprintf(details[0].(string), details[1:]...)
	}

	rf.logger.Info(ev + " " + det)

	tag := fmt.Sprintf("[Server %d]", rf.me)

	tester.Annotate(tag, ev, det)
}

type Votes struct {
	mu      sync.Mutex
	counter int
	total   int
}

func NewVotes(total int) *Votes {
	v := Votes{
		counter: 0,
		total:   total,
	}
	return &v
}

func (v *Votes) Vote() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.counter++
}

func (v *Votes) HasMajority() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.counter > (v.total / 2)
}

func (v *Votes) Reset() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.counter = 0
}
