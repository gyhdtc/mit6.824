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
	"sync"
	"sync/atomic"
	"time"
	"bytes"
	"encoding/gob"
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
type LogEntry struct {
	Command interface{}
	Term    int
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
	State           int // 状态 F C L
	ElectionTimeout int // 超时选举

	/* chan 将会在 select 中进行等待，发生一个，其他的都不会触发，还要在 server（）函数中进行重置 */
	/* Follower ： 超时、心跳、投票 / Candidate ： 超时、心跳、当选 / Leader ： 发心跳、sleep（） */
	HeartBeatNotify   chan bool // 心跳通知 （1）
	VoteNotify        chan bool // 投票通知 （2）
	ElectLeaderNotify chan bool // 当选 Leader 通知 （3）

	VotedCount int // 获得票数
	LeaderId   int // 当前Term的ID

	// Leader时变数据，选举后初始化
	/* 发送给每个服务器，将下一个日志条目的索引发送到该服务器(初始化为leader last log index + 1) */
	NextIndex []int
	/* 已经发送给每个服务器，在其他服务器上复制的已知的最高日志项的索引(初始化为0，单调递增) */
	MatchIndex []int

	// All Server时变数据
	CommitIndex int // 要提交给Client的最高日志项的索引(初始化为0，单调递增)
	LastApplied int // 已提交给Client的最高日志项的索引(初始化为0，单调递增)

	// 持久化数据
	CurrentTerm int        // 当前 Term
	VotedForId  int        // 候选人 ID
	Log         []LogEntry // 日志

	// 是否被 kill
	Done bool

	// 一个通道，Leader负责传递
	ClientApply chan ApplyMsg
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedForId)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedForId)
	d.Decode(&rf.Log)
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
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	LogEntries   []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) DisplayState() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch rf.State {
	case Candidate:
		return "candidate"
	case Follower:
		return "follower"
	default:
		return "leader"
	}
}

// 得到一系列通知，触发 select
func PutDataToChannel(c chan bool) {
	go func() {
		c <- true
	}()
}

// 状态转变
/* 转为 Follower，更新 Term 和 State；选后人无，得票0；当前Term的Leader */
func (rf *Raft) turnFollower(Term, LeaderId int) {
	rf.State = Follower
	rf.CurrentTerm = Term
	rf.VotedForId = -1
	rf.VotedCount = 0
	rf.LeaderId = LeaderId
	debug("====>[%d] %d server as follower", rf.CurrentTerm, rf.me)
}

/* 转为 Candidate，更新 State；选后人自己，得票 ++；当前Term ++；重置选举时间 */
func (rf *Raft) turnCandidate() {
	rf.State = Candidate
	rf.VotedForId = rf.me
	rf.VotedCount = 1
	rf.CurrentTerm++
	rf.ResetTimeOut()
	debug("====>[%d] %d server as candidate and timeout is %+v", rf.CurrentTerm, rf.me, rf.ElectionTimeout)
}

/* 转为 Leader，更新 State */
func (rf *Raft) turnLeader() {
	rf.State = Leader
	debug("====>[%d] %d server as leader", rf.CurrentTerm, rf.me)
	rf.reinitialized()
}
func (rf *Raft) reinitialized() {
	for i := range rf.peers {
		rf.NextIndex[i] = len(rf.Log)
		rf.MatchIndex[i] = 0
	}
}

//
// 【主循环】
// 根据 raft 的状态来进行相应的 server_as...
//
func (rf *Raft) server() {
	for !rf.isDone() {
		rf.precheck()
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
	rf.SendAppendEntries()
	time.Sleep(100 * time.Millisecond)
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

/* 检查，提交command */
func (rf *Raft) precheck() {
	rf.mu.Lock()
	if rf.CommitIndex > rf.LastApplied {
		lastapplyindex := rf.LastApplied
		CommitIndex := rf.CommitIndex
		log := rf.Log
		rf.LastApplied = rf.CommitIndex
		rf.mu.Unlock()
		rf.Apply(log, lastapplyindex+1, CommitIndex)
	} else {
		rf.mu.Unlock()
	}
}
func (rf *Raft) Apply(log []LogEntry, start, end int) {
	for i := start; i <= end; i++ {
		debug("======>server %d role %s:commit log %+v at index %d", rf.me, rf.DisplayState(), log[i].Command, i)
		rf.ClientApply <- ApplyMsg{
			CommandIndex: i,
			Command:      log[i].Command,
			CommandValid: true,
		}
	}
}

/* 判断能否投票；实验2B要加上 Log 的判断 */
/* return rf.agreeTerm(candidateId) && rf.agreeLog(...) */
func (rf *Raft) CanVote(candidateId, candidateTerm, candidateLogIndex int) bool {
	return rf.agreeTerm(candidateId) && rf.agreeLog(candidateTerm, candidateLogIndex)
}
func (rf *Raft) agreeTerm(candidateId int) bool {
	return rf.VotedForId < 0 || rf.VotedForId == candidateId
}
func (rf *Raft) agreeLog(candidateTerm, candidateLogIndex int) bool {
	lastLog := rf.Log[len(rf.Log)-1]
	return lastLog.Term < candidateTerm || (lastLog.Term == candidateTerm && len(rf.Log)-1 <= candidateLogIndex)
}

/* 一些辅助函数 */

// 接收、处理
/* 接收投票 */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.CurrentTerm {
		// 如果 args.Term > rf.CurrentTerm，那么就要为其投票了
		// 在下面进行状态转换之后，可以确保一定为其投票
		rf.turnFollower(args.Term, NoLeader)
	}
	/* 如果 args.Term = rf.CurrentTerm 那么是不能转变状态为 Follower 的 */
	/* 因为他们处于一种状态，只有判断能否为他投票 */
	/* 其中就蕴含了，如果是两个Node同时转变为 Candidate，将无法为其投票 */
	if rf.CanVote(args.CandidateId, args.LastLogTerm, args.LastLogIndex) {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		rf.VotedForId = args.CandidateId
		PutDataToChannel(rf.VoteNotify)
	} else {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	}
}

/* 接收投票 */

/* 接收心跳 */
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()

	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	PutDataToChannel(rf.HeartBeatNotify)
	/* 稳定状态应该是，我是 Follower，并且我的 Term 等于 Leader */
	if !(rf.State == Follower && rf.CurrentTerm == args.Term) {
		rf.turnFollower(args.Term, args.LeaderId)
	}
	//------
	/* 接收到心跳包中的 pre-term 和 pre-index */
	/* 我们需要找到 follower 中的与之对应的term，一般是最后一个 */
	/* 找到了，在 follower日至中的第 i 个，返回true，删除 i 之后的，复制日至 */
	/* 没找到，返回false，leader中的 nextindex -- */
	//------
	if ((len(rf.Log) - 1) < args.PreLogIndex) || (rf.Log[args.PreLogIndex].Term != args.PreLogTerm) {
		reply.Term = args.Term
		reply.Success = false
		return
	}
	if len(args.LogEntries) > 0 {
		rf.Log = rf.Log[:args.PreLogIndex+1]
		for _, item := range args.LogEntries {
			rf.Log = append(rf.Log, item)
			debug("======>server %d role ---:copy log %+v at index %d", rf.me, item, len(rf.Log)-1)
		}
	}
	// ---
	// 如何处理日至 commit 
	// ---
	if args.LeaderCommit > rf.CommitIndex {
		if args.LeaderCommit > len(rf.Log) - 1 {
			rf.CommitIndex = len(rf.Log) - 1
		} else {
			rf.CommitIndex = args.LeaderCommit
		}
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
				Term:         rf.CurrentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.Log) - 1,
				LastLogTerm:  rf.Log[len(rf.Log)-1].Term,
			}
			go func(args RequestVoteArgs, i int) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(i, &args, &reply)
				rf.mu.Lock()
				if rf.State == Candidate {
					if ok {

						if reply.VoteGranted {
							rf.VotedCount++
						}
					}
					if rf.VotedCount > len(rf.peers)/2 {
						rf.turnLeader()
						PutDataToChannel(rf.ElectLeaderNotify)
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
			if rf.NextIndex[i] <= len(rf.Log)-1 {
				args := AppendEntriesArgs{
					Term:         rf.CurrentTerm,
					LeaderId:     rf.me,
					PreLogIndex:  rf.NextIndex[i] - 1,
					PreLogTerm:   rf.Log[rf.NextIndex[i]-1].Term,
					LogEntries:   rf.Log[rf.NextIndex[i]:],
					LeaderCommit: rf.CommitIndex,
				}
				go func(args AppendEntriesArgs, i int) {
					var reply AppendEntriesReply
					ok := rf.sendAppendEntries(i, &args, &reply)

					rf.mu.Lock()
					if ok {
						if reply.Success {

							rf.MatchIndex[i] = rf.NextIndex[i] + len(args.LogEntries) - 1
							rf.NextIndex[i] = rf.MatchIndex[i] + 1

							for n := rf.CommitIndex + 1; n < len(rf.Log); n++ {
								numOfcopy := 0
								for m := range rf.peers {
									if rf.MatchIndex[m] >= n {
										numOfcopy++
									}
									if numOfcopy > len(rf.peers)/2 {
										rf.CommitIndex = n
									}
								}
							}

						} else if reply.Term > args.Term {

							rf.turnFollower(reply.Term, NoLeader)

						} else if reply.Term == args.Term {
							/* Term和我的相等，却拒绝了我的entry，说明发生了日至冲突 */
							rf.NextIndex[i]--
						}
					}
					rf.mu.Unlock()
				}(args, i)
			} else {
				// 普通心跳
				args := AppendEntriesArgs{
					Term:         rf.CurrentTerm,
					LeaderId:     rf.me,
					PreLogIndex:  rf.NextIndex[i] - 1,
					PreLogTerm:   rf.Log[rf.NextIndex[i]-1].Term,
					LeaderCommit: rf.CommitIndex,
				}
				go func(args AppendEntriesArgs, i int) {
					var reply AppendEntriesReply
					ok := rf.sendAppendEntries(i, &args, &reply)

					rf.mu.Lock()
					if ok {
						if reply.Success {

							/* 非entry心跳，不需要处理啥 */

						} else if reply.Term > args.Term {

							rf.turnFollower(reply.Term, NoLeader)

						} else if reply.Term == args.Term {
							/* Term和我的相等，却拒绝了我的entry，说明发生了日至冲突 */
							rf.NextIndex[i]--
						}
					}
					rf.mu.Unlock()
				}(args, i)
			}

		} else {
			// -------
			// 这里没懂
			// -------
			rf.NextIndex[i]++
			rf.MatchIndex[i]++
		}
	}
	rf.mu.Unlock()
}

/* 发送心跳 */
// 发送

// Debug 函数
const EnableDebug = false

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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.mu.Lock()
	rf.mu.Unlock()

	isLeader = rf.State == Leader
	term = rf.CurrentTerm
	index = len(rf.Log)
	if !isLeader {
		return index, term, isLeader
	}
	rf.Log = append(rf.Log, LogEntry{
		Command: command,
		Term:    term,
	})

	rf.persist()

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
	/* ElectionTimeout */
	rf.ResetTimeOut()
	/* State，CurrentTerm, VoteForId, VoteCount, LeaderId */
	rf.turnFollower(0, NoLeader)

	rf.HeartBeatNotify = make(chan bool)
	rf.VoteNotify = make(chan bool)
	rf.ElectLeaderNotify = make(chan bool)
	rf.ClientApply = applyCh
	rf.Log = []LogEntry{{}}
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
	rf.Done = false
	rf.CommitIndex = 0
	rf.LastApplied = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.server()
	return rf
}
