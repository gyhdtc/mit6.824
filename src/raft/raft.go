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

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 时变数据
	State             int       // 状态 F C L
	ElectionTimeout   int       // 超时选举
	HeartBeatNotify   chan bool // 心跳通知 （1）
	VoteNotify        chan bool // 投票通知 （2）
	ElectLeaderNotify chan bool // 当选 Leader 通知 （3）
	HeartTimeout      chan bool
	HeartOrNot		  bool
	Flag 			  int
	/* 上述 chan 将会在 select 中进行等待，发生一个，其他的都不会触发，还要在 server（）函数中进行重置 */
	/* Follower ： 超时、心跳、投票 */
	/* Candidate ： 超时、心跳、当选 */
	/* Leader ： 发心跳、sleep（） */
	VotedCount int // 获得票数
	LeaderId   int

	// 持久化数据
	CurrentTerm int // 当前 Term
	VotedForId  int // 候选人 ID
	// 是否被 kill
	Done bool
}

const (
	NoLeader  = -1
	Leader    = 0
	Candidate = 1
	Follower  = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.State == Leader
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}
type AppendEntriesReply struct {
	Term     int
	Success  bool
	LeaderId int
}

// 得到一些列通知，触发 select
func (rf *Raft) getVoteRequest() {
	rf.VoteNotify <- true
}
func (rf *Raft) getHeartBeat() {
	rf.HeartBeatNotify <- true
}
func (rf *Raft) becomeLeader() {
	rf.ElectLeaderNotify <- true
}

// 状态转变
/* 转为 Follower，更新 Term 和 State；选后人无，得票0；当前Term的Leader */
func (rf *Raft) turnFollower(Term, LeaderId int) {
	rf.State = Follower
	rf.CurrentTerm = Term
	rf.VotedForId = -1
	rf.VotedCount = 0
	rf.LeaderId = LeaderId
	//debug("===> _%d_ (%d) follower =%d= *%d*", rf.me, rf.ElectionTimeout, rf.CurrentTerm, rf.VotedForId)
}

/* 转为 Candidate，更新 State；选后人自己，得票 ++；当前Term ++；重置选举时间 */
func (rf *Raft) turnCandidate() {
	rf.State = Candidate
	rf.VotedForId = rf.me
	rf.VotedCount++
	rf.CurrentTerm++
	rf.ResetTimeOut()
	debug("===> _%d_ (%d) candidate =%d= *%d*", rf.me, rf.ElectionTimeout, rf.CurrentTerm, rf.VotedForId)
}

/* 转为 Leader，更新 State */
func (rf *Raft) turnLeader() {
	rf.State = Leader
	debug("===> _%d_ (%d) leader =%d= *%d*", rf.me, rf.ElectionTimeout, rf.CurrentTerm, rf.VotedForId)
}

//
// 【主循环】
// 根据 raft 的状态来进行相应的 server_as...
//
func (rf *Raft) server() {
	for !rf.isDone() {
		switch rf.syncState() {
		case Leader:
			rf.serverAsleader()
		case Candidate:
			rf.serverAscandidate()
		case Follower:
			rf.serverAsfollower()
		}
	}
}
func (rf *Raft) serverAsfollower() {
	select {
	case <-time.Tick(time.Millisecond * time.Duration(rf.syncTimeOut())):
		rf.mu.Lock()
		rf.turnCandidate()
		rf.mu.Unlock()
	case <-rf.HeartBeatNotify:
	case <-rf.VoteNotify:
	}
}
func (rf *Raft) serverAscandidate() {
	rf.SendRequestVote()
	select {
	case <-time.Tick(time.Millisecond * time.Duration(rf.syncTimeOut())):
		rf.mu.Lock()
		rf.turnCandidate()
		rf.mu.Unlock()
	case <-rf.HeartBeatNotify:
	case <-rf.ElectLeaderNotify:
	}
}
func (rf *Raft) serverAsleader() {
	//len := len(rf.peers)
	rf.SendAppendEntries()
	time.Sleep(150 * time.Millisecond)

	// for i := 0; i < len(rf.peers)-1; i++ {
	// 	rf.HeartTimeout <- true
	// }
	
	// i := 0
	// for i == 0 {
	// 	select {
	// 	case <- time.Tick(time.Millisecond * time.Duration(70)):
	// 		i = 1
	// 	case rf.HeartTimeout <- true:
	// 	}
	// }
	// <- rf.HeartTimeout
}

//
// example RequestVote RPC handler.
//

/* 一些辅助函数 */
func (rf *Raft) syncState() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.State
}
func (rf *Raft) ResetTimeOut() {
	rf.ElectionTimeout = rand.Intn(150) + 150
}
func (rf *Raft) syncTimeOut() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.ElectionTimeout
}

/* 判断能否投票；实验2B要加上 Log 的判断 */
/* return rf.agreeTerm(candidateId) && rf.agreeLog(...) */
func (rf *Raft) CanVote(candidateId int) bool {
	return rf.agreeTerm(candidateId)
}
func (rf *Raft) agreeTerm(candidateId int) bool {
	return rf.VotedForId < 0 || rf.VotedForId == candidateId
}

/* 一些辅助函数 */

// 接收、处理
/* 接收投票 */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.turnFollower(args.Term, NoLeader)
	}
	/* args.Term == rf.CurrentTerm */
	/* can not trun follower, because they are same level */
	/* If my VoteFor is not candidater, then i should not vote him */
	/* Be sure my VoteFor is -1 noleader */
	if rf.CanVote(args.CandidateId) {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		rf.VotedForId = args.CandidateId
		rf.getVoteRequest()
	} else {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	}
}

/* 接收投票 */

/* 接收心跳 */
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		reply.LeaderId = rf.LeaderId
		return
	}

	rf.getHeartBeat()

	/* 稳定状态应该是，我是 Follower，并且我的 Term 等于 Leader */

	if !(rf.CurrentTerm == args.Term && rf.State == Follower) {
		rf.turnFollower(args.Term, args.LeaderId)
	}

	reply.Term = rf.CurrentTerm
	reply.Success = true

	return
}

/* 接收心跳 */
// 接收、处理

// 发送
/* 发送投票 */
func (rf *Raft) SendRequestVote() {
	rf.mu.Lock()
	for i := range rf.peers {
		if i != rf.me {
			args := RequestVoteArgs{
				Term:        rf.CurrentTerm,
				CandidateId: rf.me,
			}
			go func(args RequestVoteArgs, i int) {
				var reply RequestVoteReply
				debug("===> _%d_[%d] ---send vote req---> _%d_", rf.me, rf.CurrentTerm, i)
				ok := rf.sendRequestVote(i, &args, &reply)
				rf.mu.Lock()
				if rf.State == Candidate {
					if ok {
						debug("===> _%d_[%d] <--get vote reply--- [%d] %s", rf.me, rf.CurrentTerm, reply.Term, strconv.FormatBool(reply.VoteGranted))
						if reply.VoteGranted {
							rf.VotedCount++
						}
					} else {
						debug("===> _%d_[%d] ---not send vote req---> [%d]", rf.me, rf.CurrentTerm, i)
					}
					if rf.VotedCount > len(rf.peers)/2 {
						rf.turnLeader()
						rf.becomeLeader()
					}
				}
				rf.mu.Unlock()
			}(args, i)
		}
	}
	rf.mu.Unlock()
}

/* 发送投票 */

/* 发送心跳 */
func (rf *Raft) SendAppendEntries() {
	rf.mu.Lock()
	for i := range rf.peers {
		if i != rf.me {
			args := AppendEntriesArgs{
				Term:     rf.CurrentTerm,
				LeaderId: rf.me,
			}
			go func(args AppendEntriesArgs, i int) {
				var reply AppendEntriesReply
				
				// ok := rf.sendAppendEntries(i, &args, &reply)
				// debug("===> _%d_[%d] ---send heart---> _%d_", rf.me, rf.CurrentTerm, i)
				// rf.mu.Lock()
				// if ok {
				// 	if reply.Success {
				// 		debug("===> _%d_[%d] <---heart--- _%d_[%d]", rf.me, rf.CurrentTerm, i, reply.Term)
				// 	} else {
				// 		debug("===> _%d_[%d] <---refuse heart--- _%d_[%d]", rf.me, rf.CurrentTerm, i, reply.Term)
				// 		rf.turnFollower(reply.Term, reply.LeaderId)
				// 	}
				// } else {
				// 	debug("===> _%d_[%d] ---not send heart---> _%d_", rf.me, rf.CurrentTerm, i)
				// }
				// rf.mu.Unlock()
				debug("XXX %d", rf.me)
				select {
				case ok := <- rf.sendAppendEntries(i, &args, &reply):
					{
						debug("===> _%d_[%d] ---send heart---> _%d_", rf.me, args.Term, i)	
						rf.mu.Lock()
						if ok {
							if reply.Success {
								debug("===> _%d_[%d] <---heart--- _%d_[%d]", rf.me, args.Term, i, reply.Term)
							} else {
								debug("===> _%d_[%d] <---refuse heart--- _%d_[%d]", rf.me, args.Term, i, reply.Term)
								rf.turnFollower(reply.Term, reply.LeaderId)
							}
						} else {
							debug("===> _%d_[%d] ---not send heart---> _%d_", rf.me, args.Term, i)
						}
						rf.mu.Unlock()
					}
				case <- time.Tick(time.Duration(1) * time.Millisecond):
					{
						debug("===> _%d_[%d] |||| _%d_", rf.me, rf.CurrentTerm, i)
					}
				}
			}(args, i)
		}
	}
	rf.mu.Unlock()
}

/* 发送心跳 */
// 发送

// Debug 函数
const EnableDebug = true

func debug(format string, a ...interface{}) {
	if EnableDebug {
		fmt.Printf(format+"\n\n", a...)
	}
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
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) chan bool {
	ok := make(chan bool, 1)
	ok <- rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//

func (rf *Raft) isDone() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.Done
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.Done = true
	debug("[%d] has been killed -------------", rf.me)
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	// Your initialization code here (2A, 2B, 2C).
	rf.ResetTimeOut()
	rf.turnFollower(0, NoLeader)
	rf.HeartBeatNotify = make(chan bool)
	rf.VoteNotify = make(chan bool)
	rf.ElectLeaderNotify = make(chan bool)
	rf.HeartTimeout = make(chan bool, 1)
	rf.Done = false
	rf.Flag = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.server()
	return rf
}
