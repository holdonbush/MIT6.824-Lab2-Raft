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
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
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
	currentTerm		int           // Server当前的term
	voteFor			int           // Server在选举阶段的投票目标
	logs            []LogEntry
	nextIndexs      []int         // Leader在发送LogEntry时，对应每个其他Server，开始发送的index
	matchIndexs     []int
	commitIndex     int           // Server已经commit了的Log index
	lastApplied     int           // Server已经apply了的log index
	myStatus        Status        // Server的状态

	timer           *time.Ticker  // timer
	voteTimeout     time.Duration // 选举超时时间，选举超时时间是会变动的，所以定义在Raft结构体中
	applyChan       chan ApplyMsg // 消息channel

	// 2D
	lastIncludeIndex  int         // snapshot保存的最后log的index
	lastIncludeTerm   int         // snapshot保存的最后log的term
	snapshotCmd       []byte
}

// LogEntry
type LogEntry struct {
	Term    int                // LogEntry中记录有log的Term
	Cmd     interface{}        // Log的command
}

// 定义一个全局心跳超时时间
var HeartBeatTimeout = 120*time.Millisecond

type Status int64
const (
	Follower Status = iota
	Candidate
	Leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	// 获取Server当前的Term和是否是Leader
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.myStatus == Leader
	rf.mu.Unlock()
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
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
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
	d := labgob.NewDecoder(r)
	var tmpTerm int
	var tmpVoteFor int
	var tmplogs []LogEntry
	if d.Decode(&tmpTerm) != nil ||
		d.Decode(&tmpVoteFor) != nil ||
		d.Decode(&tmplogs) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = tmpTerm
		rf.voteFor = tmpVoteFor
		rf.logs = tmplogs
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// 在lastIncludeIndex后有新的内容，返回false
	rf.mu.Lock()
	if len(rf.logs)+rf.lastIncludeIndex > lastIncludedIndex {
		return false
	}

	rf.snapshotCmd = snapshot
	// 当前log最大index小于等于snapshot中存储的最后一个index
	rf.logs = []LogEntry{}
	rf.lastIncludeTerm = lastIncludedTerm
	rf.lastIncludeIndex = lastIncludedIndex

	rf.commitIndex = rf.lastIncludeIndex
	rf.lastApplied = rf.commitIndex
	rf.mu.Unlock()
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return
	}
	pos := index - rf.lastIncludeIndex-1
	rf.lastIncludeIndex = index
	rf.lastIncludeTerm = rf.logs[pos].Term
	rf.logs = rf.logs[pos+1:]
	rf.snapshotCmd = snapshot
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type VoteErr int64
const (
	Nil  VoteErr = iota     //投票过程无错误
	VoteReqOutofDate        //投票消息过期
	CandidateLogTooOld      //候选人Log不够新
	VotedThisTerm           //本Term内已经投过票
	RaftKilled              //Raft程已终止
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term    		 int
	Candidate 		 int
	LastLogIndex 	 int    // 用于选举限制，LogEntry中最后Log的index
	LastLogTerm      int    // 用于选举限制，LogEntry中最后log的Term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term     		int
	VoteGranted     bool     //是否同意投票
	VoteErr 	    VoteErr  //投票操作错误
}

type AppendEntriesErr int64
const (
	AppendErr_Nil AppendEntriesErr = iota    // Append操作无错误
	AppendErr_LogsNotMatch                   // Append操作log不匹配
	AppendErr_ReqOutofDate                   // Append操作请求过期
	AppendErr_ReqRepeat                      // Append请求重复
	AppendErr_Commited                       // Append的log已经commit
	AppendErr_RaftKilled                     // Raft程序终止
 )

type AppendEntriesArgs struct {
	Term   				 int
	LeaderId  			 int            //Leader标识
	PrevLogIndex  		 int            //nextIndex前一个index
	PrevLogTerm    		 int            //nextindex前一个index处的term
	Logs    			 []LogEntry
	LeaderCommit  		 int            //Leader已经commit了的Log index
	LogIndex  			 int
}

type AppendEntriesReply struct {
	Term       int
	Success    bool              // Append操作结果
	AppendErr  AppendEntriesErr  // Append操作错误情况
	NotMatchIndex  int           // 当前Term的第一个元素（没有被commit的元素）的index
}

// snapshot
type InstallSnapshotRequest struct {
	Term       		 int
	LeaderId   		 int
	LastIncludeIndex int
	LastIncludeTerm  int
	//Offset         int        // Lab2D不要求实现
	Data         	 []byte
	//Done         	 bool       // Lab2D不要求实现
}

type InstallSnapshotErr int64
const (
	InstallSnapshotErr_Nil InstallSnapshotErr = iota
	InstallSnapshotErr_ReqOutofDate
	InstallSnapshotErr_OldIndex
)
type InstallSnapshotResponse struct {
	Term         int
	Err          InstallSnapshotErr
}

//
// example RequestVote RPC handler.
//
// 投票过程
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		reply.VoteErr = RaftKilled
		return
	}
	rf.mu.Lock()
	if args.Term < rf.currentTerm {      // 请求term更小，不投票
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		reply.VoteErr = VoteReqOutofDate
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.myStatus = Follower
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}
	// 选举限制-参数term小于自身的term
	candidateLogTermTooOld :=args.LastLogTerm < rf.lastIncludeTerm || (len(rf.logs) > 0 && args.LastLogTerm < rf.logs[len(rf.logs)-1].Term)    // 有log，取最后一条log的term比较 || 无log时参数term小于snapshot的term
	// 选举限制-term相等，参数index小于自身index
	candidateLogIndexTooOld :=  (args.LastLogIndex < rf.lastIncludeIndex) || (len(rf.logs) > 0 && args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < len(rf.logs)+rf.lastIncludeIndex) //有log，取最后一条log的index比较 || 无log时取snapshot的index比较

	// 选举限制
	if candidateLogIndexTooOld || candidateLogTermTooOld {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		reply.VoteGranted = false
		reply.VoteErr = CandidateLogTooOld
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if args.Term == rf.currentTerm {
		reply.Term = args.Term
		// 已经投过票,且投给了同一人,由于某些原因，之前的resp丢失
		if rf.voteFor == args.Candidate {
			rf.myStatus = Follower
			rf.timer.Reset(rf.voteTimeout)
			reply.VoteGranted = true
			reply.VoteErr = VotedThisTerm
			rf.mu.Unlock()
			return
		}
		// 来自同一Term不同Candidate的请求，忽略
		if rf.voteFor != -1 {
			reply.VoteGranted = false
			reply.VoteErr = VotedThisTerm
			rf.mu.Unlock()
			return
		}
	}

	// 可以投票
	rf.currentTerm = args.Term
	rf.voteFor = args.Candidate
	rf.myStatus = Follower
	rf.timer.Reset(rf.voteTimeout)

	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	reply.VoteErr = Nil
	rf.persist()
	rf.mu.Unlock()
	return
}

// 心跳包/log追加
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)  {
	if rf.killed() {
		reply.Term = -1
		reply.AppendErr = AppendErr_RaftKilled
		reply.Success = false
		return
	}
	rf.mu.Lock()
	// 无效消息
	if args.Term < rf.currentTerm || args.PrevLogIndex < rf.lastIncludeIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.AppendErr = AppendErr_ReqOutofDate
		reply.NotMatchIndex = -1
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	rf.voteFor = args.LeaderId
	rf.myStatus = Follower
	rf.timer.Reset(rf.voteTimeout)

	// 不匹配
	if (args.PrevLogIndex != rf.lastIncludeIndex && (args.PrevLogIndex >= len(rf.logs)+rf.lastIncludeIndex+1 || args.PrevLogTerm != rf.logs[args.PrevLogIndex-rf.lastIncludeIndex-1].Term)) ||
		(args.PrevLogIndex == rf.lastIncludeIndex && args.PrevLogTerm != rf.lastIncludeTerm){
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.AppendErr = AppendErr_LogsNotMatch
		reply.NotMatchIndex = rf.lastApplied + 1
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if rf.lastApplied > args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.AppendErr = AppendErr_Commited
		reply.NotMatchIndex = rf.lastApplied+1
		rf.persist()
		rf.mu.Unlock()
		return
	}

	// 处理日志
	if args.Logs != nil {
		rf.logs = rf.logs[:args.PrevLogIndex-rf.lastIncludeIndex]
		rf.logs = append(rf.logs, args.Logs...)
	}
	for rf.lastApplied < args.LeaderCommit {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command: rf.logs[rf.lastApplied-rf.lastIncludeIndex-1].Cmd,
		}
		rf.applyChan <- applyMsg
		rf.commitIndex = rf.lastApplied
	}


	reply.Term = rf.currentTerm
	reply.Success = true
	reply.AppendErr = AppendErr_Nil
	reply.NotMatchIndex = -1
	rf.persist()
	rf.mu.Unlock()
	return
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotResponse) {
	if rf.killed() {
		reply.Term = args.Term
		return
	}

	rf.mu.Lock()
	//fmt.Println(rf.me,"收到snapshot","来自",args.LeaderId,"自身lastinclueindex",rf.lastIncludeTerm, args.LastIncludeIndex)
	// defer rf.mu.Unlock()

	// 过期消息
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		//fmt.Println("installsnapshot消息过期")
		reply.Err = InstallSnapshotErr_ReqOutofDate
		rf.mu.Unlock()
		return
	}

	// 错误消息
	if args.LastIncludeIndex <= rf.lastIncludeIndex {
		reply.Term = rf.currentTerm
		//fmt.Println("installsnapsho消息lastIncludeIndex错误")
		reply.Err = InstallSnapshotErr_OldIndex
		rf.timer.Reset(rf.voteTimeout)
		rf.mu.Unlock()
		return
	}
	// 创建快照
	rf.currentTerm = args.Term
	rf.voteFor = args.LeaderId
	rf.myStatus = Follower
	rf.timer.Reset(rf.voteTimeout)

	if len(rf.logs)+rf.lastIncludeIndex <= args.LastIncludeIndex {
		rf.logs = []LogEntry{}
		rf.lastIncludeIndex = args.LastIncludeIndex
		rf.lastIncludeTerm = args.LastIncludeTerm
	} else {
		//  rf.logs = rf.logs[len(rf.logs)+rf.lastIncludeIndex-args.LastIncludeIndex:]
		rf.logs = rf.logs[args.LastIncludeIndex-rf.lastIncludeIndex:]
		rf.lastIncludeIndex = args.LastIncludeIndex
		rf.lastIncludeTerm = args.LastIncludeTerm
	}

	rf.applyChan <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludeIndex,
	}
	rf.lastApplied = args.LastIncludeIndex
	rf.commitIndex = rf.lastApplied

	reply.Term = rf.currentTerm
	reply.Err = InstallSnapshotErr_Nil
	rf.mu.Unlock()
	return
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
// 改造函数，添加了一个参数，用于方便实现同一Term内请求的统计
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNum *int) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		// 失败重传
		if rf.killed() {
			return false
		}
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok {
			break
		}
	}

	if rf.killed() {
		return false
	}
	rf.mu.Lock()
	if args.Term < rf.currentTerm {   // 过期请求
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()

	switch reply.VoteErr {
	case VoteReqOutofDate:
		rf.mu.Lock()
		rf.myStatus = Follower
		rf.timer.Reset(rf.voteTimeout)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.voteFor = -1
			rf.persist()
		}
		rf.mu.Unlock()
	case CandidateLogTooOld:
		// 日志不够新
		rf.mu.Lock()
		rf.myStatus = Follower
		rf.timer.Reset(rf.voteTimeout)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.voteFor = -1
			rf.persist()
		}
		rf.mu.Unlock()
	case Nil,VotedThisTerm:
		rf.mu.Lock()
		//根据是否同意投票，收集选票数量
		if reply.VoteGranted && reply.Term == rf.currentTerm && *voteNum <= len(rf.peers)/2 {
			*voteNum++
		}
		if *voteNum > len(rf.peers)/2 {
			*voteNum = 0
			if rf.myStatus == Leader {
				rf.mu.Unlock()
				return ok
			}
			rf.myStatus = Leader
			rf.nextIndexs = make([]int, len(rf.peers))
			for i,_ := range rf.nextIndexs {
				rf.nextIndexs[i] = len(rf.logs)+rf.lastIncludeIndex+1
			}
			rf.timer.Reset(HeartBeatTimeout)
		}
		rf.mu.Unlock()
	case RaftKilled:
		return false
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNum *int) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if ok {
			break
		}
	}

	if rf.killed() {
		return false
	}
	rf.mu.Lock()
	if args.Term < rf.currentTerm { // 过期消息
		rf.mu.Unlock()
		return false
	}

	switch reply.AppendErr {
	case AppendErr_Nil:
		if reply.Success && reply.Term == rf.currentTerm && *appendNum <= len(rf.peers)/2 {
			*appendNum++
		}
		if rf.nextIndexs[server] > args.LogIndex+1 {
			rf.mu.Unlock()
			return ok
		}
		rf.nextIndexs[server] = args.LogIndex+1
		if *appendNum > len(rf.peers)/2 {
			*appendNum = 0
			if (args.LogIndex>rf.lastIncludeIndex && rf.logs[args.LogIndex-rf.lastIncludeIndex-1].Term != rf.currentTerm) ||
				(args.LogIndex == rf.lastIncludeIndex && rf.lastIncludeTerm != rf.currentTerm){
				rf.mu.Unlock()
				return false
			}
			for rf.lastApplied < args.LogIndex {
				rf.lastApplied++
				applyMsg := ApplyMsg{
					CommandValid:  true,
					Command:       rf.logs[rf.lastApplied-rf.lastIncludeIndex-1].Cmd,
					CommandIndex:  rf.lastApplied,
				}
				rf.applyChan <- applyMsg
				rf.commitIndex = rf.lastApplied
			}
		}
	case AppendErr_ReqOutofDate:
		rf.myStatus = Follower
		rf.timer.Reset(rf.voteTimeout)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.voteFor = -1
			rf.persist()
		}
	case AppendErr_LogsNotMatch:
		if args.Term != rf.currentTerm {
			rf.mu.Unlock()
			return false
		}
		rf.nextIndexs[server] = reply.NotMatchIndex
	case AppendErr_ReqRepeat:
		if reply.Term > rf.currentTerm {
			rf.myStatus = Follower
			rf.currentTerm = reply.Term
			rf.voteFor = -1
			rf.timer.Reset(rf.voteTimeout)
			rf.persist()
		}
	case AppendErr_Commited:
		if args.Term != rf.currentTerm {
			rf.mu.Unlock()
			return false
		}
		rf.nextIndexs[server] = reply.NotMatchIndex
	case AppendErr_RaftKilled:
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotResponse) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		if ok {
			break
		}
	}

	if rf.killed() {
		return false
	}
	rf.mu.Lock()
	if reply.Term < rf.currentTerm {
		rf.mu.Unlock()
		return false
	}
	switch reply.Err {
	case InstallSnapshotErr_Nil:
		if reply.Term > rf.currentTerm {
			rf.myStatus = Follower
			rf.currentTerm = reply.Term
			rf.voteFor = -1
			rf.timer.Reset(rf.voteTimeout)
			rf.persist()
		}
		rf.nextIndexs[server] = args.LastIncludeIndex+1
	case InstallSnapshotErr_OldIndex:
		if reply.Term > rf.currentTerm {
			rf.myStatus = Follower
			rf.currentTerm = reply.Term
			rf.voteFor = -1
			rf.timer.Reset(rf.voteTimeout)
			rf.persist()
		}
		rf.nextIndexs[server] = len(rf.logs)+rf.lastIncludeIndex+1
	case InstallSnapshotErr_ReqOutofDate:
	}


	rf.mu.Unlock()
	return false
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
	// 客户端的log
	if rf.killed() {
		return index, term, false
	}
	rf.mu.Lock()
	isLeader = rf.myStatus == Leader
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	logEntry := LogEntry{Term: rf.currentTerm, Cmd:  command}
	rf.logs = append(rf.logs, logEntry)

	index = len(rf.logs)+rf.lastIncludeIndex
	term = rf.currentTerm
	rf.persist()
	rf.mu.Unlock()

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
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			currStatus := rf.myStatus
			switch currStatus {
			case Follower:
				rf.myStatus = Candidate
				fallthrough
			case Candidate:
				// 进行选举
				rf.currentTerm+=1
				rf.voteFor = rf.me
				// 每轮选举开始时，重新设置选举超时
				rf.voteTimeout = time.Duration(rand.Intn(150)+200)*time.Millisecond
				voteNum := 1
				rf.persist()
				rf.timer.Reset(rf.voteTimeout)
				// 构造msg
				for i,_ := range rf.peers {
					if i == rf.me {
						continue
					}
					voteArgs := &RequestVoteArgs{
						Term:         rf.currentTerm,
						Candidate:    rf.me,
						LastLogIndex: len(rf.logs)+rf.lastIncludeIndex,
						LastLogTerm:  rf.lastIncludeTerm,
					}
					if len(rf.logs) > 0 {
						voteArgs.LastLogTerm = rf.logs[len(rf.logs)-1].Term
					}
					voteReply := new(RequestVoteReply)
					//fmt.Println("发起选举",rf.me,i,voteArgs,rf.currentTerm, rf.lastIncludeIndex, rf.lastIncludeTerm)
					go rf.sendRequestVote(i, voteArgs, voteReply, &voteNum)
				}
			case Leader:
				// 进行心跳
				appendNum := 1
				rf.timer.Reset(HeartBeatTimeout)
				// 构造msg
				for i,_ := range rf.peers {
					if i == rf.me {
						continue
					}
					appendEntriesArgs := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						Logs:         nil,
						LeaderCommit: rf.commitIndex,
						LogIndex:     len(rf.logs)+rf.lastIncludeIndex,
					}
					//installSnapshot，如果rf.nextIndex[i]小于等lastCludeIndex,则发送snapShot
					if rf.nextIndexs[i] <= rf.lastIncludeIndex {
						installSnapshotReq := &InstallSnapshotRequest{
							Term:             rf.currentTerm,
							LeaderId:         rf.me,
							LastIncludeIndex: rf.lastIncludeIndex,
							LastIncludeTerm:  rf.lastIncludeTerm,
							Data:             rf.snapshotCmd,
						}
						installSnapshotReply := &InstallSnapshotResponse{}
						//fmt.Println("installsnapshot", rf.me, i, rf.lastIncludeIndex, rf.lastIncludeTerm, rf.currentTerm, installSnapshotReq)
						go rf.sendInstallSnapshot(i, installSnapshotReq, installSnapshotReply)
						continue
					}
					for rf.nextIndexs[i] > rf.lastIncludeIndex {
						appendEntriesArgs.PrevLogIndex = rf.nextIndexs[i]-1
						if appendEntriesArgs.PrevLogIndex >= len(rf.logs)+rf.lastIncludeIndex+1 {
							rf.nextIndexs[i]--
							continue
						}
						if appendEntriesArgs.PrevLogIndex == rf.lastIncludeIndex {
							appendEntriesArgs.PrevLogTerm = rf.lastIncludeTerm
						} else {
							appendEntriesArgs.PrevLogTerm = rf.logs[appendEntriesArgs.PrevLogIndex-rf.lastIncludeIndex-1].Term
						}
						break
					}
					if rf.nextIndexs[i] < len(rf.logs)+rf.lastIncludeIndex+1 {
						appendEntriesArgs.Logs = make([]LogEntry,appendEntriesArgs.LogIndex+1-rf.nextIndexs[i])
						copy(appendEntriesArgs.Logs, rf.logs[rf.nextIndexs[i]-rf.lastIncludeIndex-1:appendEntriesArgs.LogIndex-rf.lastIncludeIndex])
					}

					appendEntriesReply := new(AppendEntriesReply)
					go rf.sendAppendEntries(i, appendEntriesArgs, appendEntriesReply, &appendNum)
				}
			}
			rf.mu.Unlock()
		}
	}
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
	rf.myStatus = Follower
	rf.voteFor = -1
	rand.Seed(time.Now().UnixNano())
	rf.voteTimeout = time.Duration(rand.Intn(150)+200)*time.Millisecond
	rf.currentTerm, rf.commitIndex, rf.lastApplied = 0,0,0
	rf.nextIndexs, rf.matchIndexs, rf.logs = nil, nil, []LogEntry{{0,nil}}
	rf.timer = time.NewTicker(rf.voteTimeout)
	rf.applyChan = applyCh

	// 2D
	rf.lastIncludeIndex = -1
	rf.lastIncludeTerm = 0
	rf.snapshotCmd = make([]byte, 0)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
