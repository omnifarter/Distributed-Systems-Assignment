package FTIvy

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type Node struct {
	id            int
	messageChan   chan Message
	pagesInCharge map[int]*int
	pageMap       map[int]Page
	pageMu        *sync.Mutex
	timedOut      bool
}

type CentralManager struct {
	CMId        int
	messageChan chan Message
	pageRecord  map[int]*Node
	copySet     map[int][]*Node
	pageQueue   map[int]([]Message)
	pageLock    map[int]*sync.Mutex

	//Fault-tolerant properties
	primaryId          int
	replicaMessageChan chan Message
	isElected          bool
	electionMu         *sync.Mutex
	isKilled           bool
	isKilledMu         *sync.Mutex
}

type Page struct {
	id    int
	value int // can be anything
}

type Message struct {
	requestType int
	page        Page
	senderId    int
	receiverId  int
	node        *Node
}

const (
	NUMBER_OF_NODES  = 10
	TOTAL_REPLICAS   = 3
	MESSAGE_TIMEOUT  = 1000
	ELECTION_TIMEOUT = 1000

	//Ivy read request
	READ_REQUEST      = 0
	READ_FOWARD       = 1
	SEND_PAGE_READ    = 2
	READ_CONFIRMATION = 3

	//Ivy write request
	WRITE_REQUEST      = 4
	INVALIDATE_PAGE    = 5
	INVALIDATE_CONFIRM = 6
	WRITE_FORWARD      = 7
	SEND_PAGE_WRITE    = 8
	WRITE_CONFIRMATION = 9

	//CM replica update
	CM_PRIMARY_WRITE = 10
	CM_REPLICA_ACK   = 11

	//CM election
	CM_START_ELECTION       = 12
	CM_REPLY_ELECTION       = 13
	CM_ANNOUNCE_COORDINATOR = 14
)

var nodeEntries []*Node
var cmEntries []*CentralManager
var primaryReplicaId = TOTAL_REPLICAS - 1 // initial primary replica
var writeFile *os.File
var startTime time.Time
var lastMachine = 0
var wg = &sync.WaitGroup{}

/*
This is run as a go routine to listen for incoming messages.
*/
func (c *CentralManager) listen() {
	for {
		select {
		case msg := <-c.messageChan: //Ivy protocol
			if c.isKilled {
				continue
			}
			switch msg.requestType {
			case READ_REQUEST:
				c.onReadRequest(msg)
			case READ_CONFIRMATION:
				c.onReadConfirmation(msg)
			case WRITE_REQUEST:
				c.onWriteRequest(msg)
			case INVALIDATE_CONFIRM:
				c.onInvalidateConfirm(msg)
			case WRITE_CONFIRMATION:
				c.onWriteConfirmation(msg)
			}

		case msg := <-c.replicaMessageChan: // RSM protocol
			if c.isKilled {
				continue
			}
			switch msg.requestType {
			case CM_PRIMARY_WRITE:
				c.onCMPrimaryWrite(msg)
			case CM_REPLICA_ACK:
				c.onCMReplicaAck(msg)
			case CM_START_ELECTION:
				c.onCMStartElection(msg)
			case CM_REPLY_ELECTION:
				c.onCMReplyElection()
			case CM_ANNOUNCE_COORDINATOR:
				c.onCMAnnounceCoordinator(msg)
			}
		}

	}
}

/*
Central manager forwards read request to current page holder
*/
func (c *CentralManager) onReadRequest(msg Message) {
	currentPageHolder := c.pageRecord[msg.page.id]
	currentPageHolder.messageChan <- Message{
		requestType: READ_FOWARD,
		page:        msg.page,
		senderId:    msg.senderId,
		receiverId:  currentPageHolder.id,
	}
}

/*
helper function to check if queue is empty
*/
func (c *CentralManager) isQueueEmpty(page Page) bool {
	defer c.pageLock[page.id].Unlock()
	c.pageLock[page.id].Lock()
	return (len(c.pageQueue[page.id]) == 0)
}

/*
helper function to get length of queue
*/
func (c *CentralManager) getQueueLength(page Page) int {
	defer c.pageLock[page.id].Unlock()
	c.pageLock[page.id].Lock()
	return len(c.pageQueue[page.id])
}

/*
helper function to add to start of queue.
*/
func (c *CentralManager) addQueue(msg Message) {
	defer c.pageLock[msg.page.id].Unlock()
	c.pageLock[msg.page.id].Lock()
	c.pageQueue[msg.page.id] = append(c.pageQueue[msg.page.id], msg)
}

/*
helper function to pop the head of queue.
*/
func (c *CentralManager) PopQueue(page Page) Message {
	defer c.pageLock[page.id].Unlock()
	c.pageLock[page.id].Lock()
	msg := c.pageQueue[page.id][0]
	c.pageQueue[page.id] = c.pageQueue[page.id][1:]
	return msg
}

/*
Central manager receives the read confirmation.
*/
func (c *CentralManager) onReadConfirmation(msg Message) {
	fmt.Printf("Replica %v: Read from machine %v finished.\n", msg.senderId, c.CMId)
	executeNextLoop(true)
}

/*
Central manager invalidates page held by other nodes, and forwards write request to current page holder
*/
func (c *CentralManager) onWriteRequest(msg Message) {
	c.addQueue(msg)
	if c.getQueueLength(msg.page) == 1 {
		for _, node := range nodeEntries {
			if node.id != msg.senderId && node.id != c.pageRecord[msg.page.id].id {
				node.messageChan <- Message{
					requestType: INVALIDATE_PAGE,
					page:        msg.page,
					senderId:    -1,
					receiverId:  msg.senderId,
				}
			}
		}

		currentPageHolder := c.pageRecord[msg.page.id]
		currentPageHolder.messageChan <- Message{
			requestType: WRITE_FORWARD,
			page:        msg.page,
			senderId:    msg.senderId,
			receiverId:  currentPageHolder.id,
		}

	}
}

/*
Central manager receives invalidate confirm message from nodes
*/
func (c *CentralManager) onInvalidateConfirm(msg Message) {
	fmt.Printf("Node %v - invalidated page %v\n", msg.senderId, msg.page.id)
}

/*
Central manager updates page holder record, and processes the next message in the queue, if any.
*/
func (c *CentralManager) onWriteConfirmation(msg Message) {
	//update ownership record
	c.pageLock[msg.page.id].Lock()
	c.pageRecord[msg.page.id] = nodeEntries[msg.senderId]
	c.pageLock[msg.page.id].Unlock()
	fmt.Printf("Replica %v: Write from machine %v finished.\n", msg.senderId, c.CMId)
	c.PopQueue(msg.page)

	//update all replicas of new page record.
	for i := 0; i < TOTAL_REPLICAS; i++ {
		if i == c.CMId {
			continue
		}
		cmEntries[i].replicaMessageChan <- Message{
			requestType: CM_PRIMARY_WRITE,
			page:        msg.page,
			node:        nodeEntries[msg.senderId],
			senderId:    c.CMId,
			receiverId:  i,
		}
	}

	if c.isQueueEmpty(msg.page) {
		executeNextLoop(false)
	} else {
		oldMessage := c.PopQueue(msg.page)
		for _, node := range nodeEntries {
			if node.id != oldMessage.senderId && node.id != c.pageRecord[oldMessage.page.id].id {
				node.messageChan <- Message{
					requestType: INVALIDATE_PAGE,
					page:        oldMessage.page,
					senderId:    -1,
					receiverId:  oldMessage.senderId,
				}
			}
		}
	}
}

/*
Non-primary replicas update their local page record sent by primary node, and sends back an acknowledgement message.
*/
func (c *CentralManager) onCMPrimaryWrite(msg Message) {
	c.pageLock[msg.page.id].Lock()
	c.pageRecord[msg.page.id] = msg.node
	c.pageLock[msg.page.id].Unlock()
	cmEntries[c.primaryId].replicaMessageChan <- Message{
		requestType: CM_REPLICA_ACK,
		page:        msg.page,
		senderId:    c.CMId,
		receiverId:  c.primaryId,
	}
}

/*
Primary replica receives acknowledgement message from other replicas.
*/
func (c *CentralManager) onCMReplicaAck(msg Message) {
	fmt.Printf("Primary CM: replica %v has updated page record\n", msg.senderId)
}

/*
Once a replica receives an election message, check if the senderId is smaller than its own id. if so, reply the message and start its own election.
*/
func (c *CentralManager) onCMStartElection(msg Message) {
	if c.CMId > msg.senderId {
		cmEntries[msg.senderId].replicaMessageChan <- Message{
			requestType: CM_REPLY_ELECTION,
			senderId:    c.CMId,
			receiverId:  msg.senderId,
		}

		go c.startElection()
	}
}

/*
This is run as a go routine. Elects itself as the coordinator. Waits for election timeout duration and if no other replica has sent a reply, broadcast to all replicas that it has won.
*/
func (c *CentralManager) startElection() {
	fmt.Printf("Replica %v - starting election...\n", c.CMId)
	// set ourselves as elected coordinator
	c.electionMu.Lock()
	c.isElected = true
	c.electionMu.Unlock()

	// send CM_START_ELECTION to everyone above our CMId
	for i := c.CMId + 1; i < TOTAL_REPLICAS; i++ {
		cmEntries[i].replicaMessageChan <- Message{
			requestType: CM_START_ELECTION,
			senderId:    c.CMId,
			receiverId:  i,
		}
	}
	// wait for election message timeout
	time.Sleep(ELECTION_TIMEOUT * time.Millisecond)

	// if we are coordinator, broadcast to everyone.
	c.electionMu.Lock()
	if c.isElected == true {
		fmt.Printf("Replica %v - won election, broadcasting...\n", c.CMId)
		c.isElected = false
		c.primaryId = c.CMId
		primaryReplicaId = c.CMId

		for i := 0; i < TOTAL_REPLICAS; i++ {
			if i == c.CMId {
				continue
			}

			cmEntries[i].replicaMessageChan <- Message{
				requestType: CM_ANNOUNCE_COORDINATOR,
				senderId:    c.CMId,
				receiverId:  i,
			}
		}

		c.electionMu.Unlock()

	}
}

/*
If the replica receives an election reply message, update local state.
*/
func (c *CentralManager) onCMReplyElection() {
	defer c.electionMu.Unlock()
	c.electionMu.Lock()
	c.isElected = false
}

/*
Replicas have received the new coordinator broadcast message, update local primary replica id.
*/
func (c *CentralManager) onCMAnnounceCoordinator(msg Message) {
	c.primaryId = msg.senderId
}

/*
Helper function to artificially kill process.
*/
func (c *CentralManager) killProcess() {
	defer c.isKilledMu.Unlock()
	c.isKilledMu.Lock()
	c.isKilled = true
}

/*
Helper function to artificially restart process.
*/
func (c *CentralManager) restartProcess() {
	defer c.isKilledMu.Unlock()
	c.isKilledMu.Lock()
	c.isKilled = false
	//TODO: start election??
	fmt.Printf("Replica %v - restarted, starting election...\n", c.CMId)
	go c.startElection()
}

/*
Run as a go routine to listen for incoming messages.
*/
func (n *Node) listen() {
	for {
		msg := <-n.messageChan

		switch msg.requestType {
		case READ_FOWARD:
			n.onReadForward(msg)
		case SEND_PAGE_READ:
			n.onSendPage(msg)

		case INVALIDATE_PAGE:
			n.onInvalidatePage(msg)
		case WRITE_FORWARD:
			n.onWriteForward(msg)
		case SEND_PAGE_WRITE:
			n.onSendPageWrite(msg)
		}
	}
}

/*
on receiving the read forward message, sends a copy of the local page to the requester.
*/
func (n *Node) onReadForward(msg Message) {
	defer n.pageMu.Unlock()
	n.pageMu.Lock()
	page := n.pageMap[msg.page.id]
	nodeEntries[msg.senderId].messageChan <- Message{
		requestType: SEND_PAGE_READ,
		page:        page,
		senderId:    n.id,
		receiverId:  msg.senderId,
	}
}

/*
Once the node receives the page from page holder, update local copy. Also, reset timeout state so that election is not triggered.
*/
func (n *Node) onSendPage(msg Message) {
	defer n.pageMu.Unlock()
	n.pageMu.Lock()
	// we ackknowledge that the CM is alive.
	n.timedOut = false
	n.pageMap[msg.page.id] = msg.page
	fmt.Printf("Node %v: Read page %v complete.\n", n.id, msg.page.id)
	cmEntries[primaryReplicaId].messageChan <- Message{
		requestType: READ_CONFIRMATION,
		page:        msg.page,
		senderId:    n.id,
		receiverId:  -1,
	}
}

/*
Invalidate local copy of page.
*/
func (n *Node) onInvalidatePage(msg Message) {
	defer n.pageMu.Unlock()
	n.pageMu.Lock()
	n.pageMap[msg.page.id] = Page{msg.page.id, 0}
	cmEntries[primaryReplicaId].messageChan <- Message{
		requestType: INVALIDATE_CONFIRM,
		page:        msg.page,
		senderId:    n.id,
		receiverId:  -1,
	}
}

/*
First copies its local page into a variable, invalidates its local page, and send the saved page to the requester.
*/
func (n *Node) onWriteForward(msg Message) {
	defer n.pageMu.Unlock()
	n.pageMu.Lock()
	//save page to forward
	page := n.pageMap[msg.page.id]
	//invalidate page and remove ownership
	n.pageMap[msg.page.id] = Page{msg.page.id, 0}
	n.pagesInCharge[msg.page.id] = nil
	nodeEntries[msg.senderId].messageChan <- Message{
		requestType: SEND_PAGE_WRITE,
		page:        page,
		senderId:    n.id,
		receiverId:  msg.senderId,
	}
}

/*
Once the node receives the latest page, increment its value by 1 to signify writing. Also, reset the timeout state to prevent election from being triggered.
*/
func (n *Node) onSendPageWrite(msg Message) {
	defer n.pageMu.Unlock()
	n.pageMu.Lock()
	// we ackknowledge that the CM is alive.
	n.timedOut = false
	// edit the page value
	page := msg.page
	page.value = page.value + 1
	//update local pageMap and claim ownership.
	n.pageMap[msg.page.id] = page
	n.pagesInCharge[page.id] = &msg.page.id

	fmt.Printf("Node %v - page %v written with value %v\n", n.id, page.id, page.value)

	cmEntries[primaryReplicaId].messageChan <- Message{
		requestType: WRITE_CONFIRMATION,
		page:        page,
		senderId:    n.id,
		receiverId:  -1,
	}
}

/*
The node first invalidates its own local copy before sending a read request to the central manager. it also spins up a go routine to listen for a timeout.
*/
func (n *Node) requestForRead(pageId int) {
	//Invalidate page
	defer n.pageMu.Unlock()
	n.pageMu.Lock()
	n.pageMap[pageId] = Page{pageId, 0}
	msg := Message{
		requestType: READ_REQUEST,
		page:        Page{id: pageId, value: 0},
		senderId:    n.id,
		receiverId:  -1,
	}
	cmEntries[primaryReplicaId].messageChan <- msg

	go n.listenForTimeout(msg)
}

/*
The node first invalidates its own local copy before sending a write request to the central manager. it also spins up a go routine to listen for a timeout.
*/
func (n *Node) requestForWrite(pageId int) {
	defer n.pageMu.Unlock()
	n.pageMu.Lock()
	fmt.Printf("Node %v - requesting for page %v write\n", n.id, pageId)
	n.pageMap[pageId] = Page{pageId, 0}
	msg := Message{
		requestType: WRITE_REQUEST,
		page:        Page{pageId, 0},
		senderId:    n.id,
		receiverId:  -1,
	}
	cmEntries[primaryReplicaId].messageChan <- msg
	go n.listenForTimeout(msg)
}

/*
Run as a go routine. If the node timeout is still set to true, we assume that the central manager is down, and call a non-primary replica to start a new election.
We give some time for the election to complete before retrying the request.
*/
func (n *Node) listenForTimeout(msg Message) {
	n.timedOut = true
	time.Sleep(MESSAGE_TIMEOUT * time.Millisecond)
	if n.timedOut {
		fmt.Printf("Node %v - Primary Replica is down, contacting the next replica to start election.\n", n.id)
		// tells one of the replicas to start election.
		go cmEntries[((primaryReplicaId + 1) % TOTAL_REPLICAS)].startElection()
		// let election finish
		time.Sleep(2000 * time.Millisecond)
		fmt.Printf("Node %v - resending the request.\n", n.id)
		// restart last command
		switch msg.requestType {
		case WRITE_REQUEST:
			n.requestForWrite(msg.page.id)
		case READ_REQUEST:
			n.requestForRead(msg.page.id)
		}
	}
}

/*
Initialises with FT-Ivy architecture with NUMBER_OF_NODES and TOTAL_REPLICAS.
*/
func initialise() {
	pageMap := make(map[int]Page)
	pageRecord := make(map[int]*Node)
	pageQueue := make(map[int][]Message)
	pageLock := make(map[int]*sync.Mutex)
	copySet := make(map[int][]*Node)
	for i := 0; i < NUMBER_OF_NODES; i++ {
		pageMap[i] = Page{i, 0}
		pageQueue[i] = make([]Message, 0)
		copySet[i] = make([]*Node, 0)
		pageLock[i] = &sync.Mutex{}
	}

	for i := 0; i < NUMBER_OF_NODES; i++ {
		pagesInCharge := make(map[int]*int)
		pagesInCharge[i] = &i

		nodeEntries = append(nodeEntries, &Node{
			id:            i,
			messageChan:   make(chan Message, 100),
			pagesInCharge: pagesInCharge,
			pageMap:       pageMap,
			pageMu:        &sync.Mutex{},
		})
		copySet[i] = append(copySet[i], nodeEntries[i])
		pageRecord[i] = nodeEntries[i]
		go nodeEntries[i].listen()
	}

	for i := 0; i < TOTAL_REPLICAS; i++ {

		cmEntries = append(cmEntries, &CentralManager{
			CMId:        i,
			messageChan: make(chan Message, 100),
			pageRecord:  pageRecord,
			pageQueue:   pageQueue,
			pageLock:    pageLock,

			primaryId:          TOTAL_REPLICAS - 1, // highest CMid is the primary replica
			replicaMessageChan: make(chan Message, 100),
			isElected:          false,
			electionMu:         &sync.Mutex{},
			isKilled:           false,
			isKilledMu:         &sync.Mutex{},
		})
		go cmEntries[i].listen()
	}
}

/*
Helper function to chain requests. This also helps to record time taken for a request to complete.
*/
func executeNextLoop(isRead bool) {
	// for experiments that do not want to loop.
	if lastMachine == -1 {
		diff := time.Since(startTime)
		writeFile.WriteString(diff.String() + "\n")
		return
	}
	if lastMachine > 0 {
		diff := time.Since(startTime)
		writeFile.WriteString(diff.String() + "\n")
	}
	if lastMachine == NUMBER_OF_NODES-2 {
		if !isRead {
			writeFile.Close()
		}
		wg.Done()
	} else {
		startTime = time.Now()
		lastMachine += 1
		if isRead {
			nodeEntries[lastMachine].requestForRead(lastMachine + 1)
		} else {
			nodeEntries[lastMachine].requestForWrite(lastMachine + 1)
		}
	}
}

/*
Experiment 1. We perform 9 read requests one at a time, and 9 write requests one at a time. The results are written into the ft_ivy_timings.txt file.
*/
func Experiment1() {
	initialise()
	writeFile, _ = os.Create("./Pset/Pset3/Experiment1/ft_ivy_timings.txt")
	writeFile.WriteString("---READ REQUESTS---" + "\n")
	wg.Add(1)
	executeNextLoop(true)
	wg.Wait()
	writeFile.WriteString("---WRITE REQUESTS---" + "\n")
	wg.Add(1)
	lastMachine = 0
	executeNextLoop(false)
	wg.Wait()
}

/*
Helper function to kill the primary replica.
*/
func KillPrimaryReplica() {
	cmEntries[primaryReplicaId].killProcess()
}

/*
Helper function to kill the primary replica for 2 seconds, before restarting the replica.
*/
func KillandRestartPrimaryReplica() {
	cmEntries[primaryReplicaId].killProcess()
	time.Sleep(2000 * time.Millisecond)
	cmEntries[primaryReplicaId].restartProcess()
}

/*
Expriment 2a. We calculate the time taken to do a write request after primary replica is down, and the time taken without failure.
The results is stored in experiment_2a_timings.txt
*/
func Experiment2a() {
	initialise()
	writeFile, _ = os.Create("./Pset/Pset3/Experiment2/experiment_2a_timings.txt")
	lastMachine = -1 // just to stop the looping

	// first we calculate the time from killing a replica till the request finishes
	startTime = time.Now()
	KillPrimaryReplica()
	nodeEntries[1].requestForWrite(2) // node 1 request for page 2.
	time.Sleep(5 * time.Second)

	// next we calculate the time taken without faults.
	startTime = time.Now()
	nodeEntries[1].requestForWrite(2) // node 1 request for page 2.
	time.Sleep(5 * time.Second)

}

/*
Expriment 2b. We calculate the time taken to do a write request when the primary replica fails for 2 seconds before restarting, and the time taken without failure.
The results is stored in experiment_2b_timings.txt
*/
func Experiment2b() {
	initialise()
	writeFile, _ = os.Create("./Pset/Pset3/Experiment2/experiment_2b_timings.txt")
	lastMachine = -1 // just to stop the looping

	// first we calculate the time from killing a replica till the request finishes
	startTime = time.Now()
	go KillandRestartPrimaryReplica()
	nodeEntries[1].requestForWrite(2) // node 1 request for page 2.
	time.Sleep(5 * time.Second)

	// next we calculate the time taken without faults.
	startTime = time.Now()
	nodeEntries[1].requestForWrite(2) // node 1 request for page 2.
	time.Sleep(5 * time.Second)
}

/*
Expriment 3. We calculate the time taken to do a write request when the primary replica keeps failing intermittently.
The results is stored in experiment_3_timings.txt
*/
func Experiment3() {
	initialise()
	writeFile, _ = os.Create("./Pset/Pset3/Experiment3/experiment_3_timings.txt")
	lastMachine = -1 // just to stop the looping

	startTime = time.Now()
	go KillandRestartPrimaryReplica()
	nodeEntries[1].requestForWrite(2) // node 1 request for page 2.
	time.Sleep(4 * time.Second)

	startTime = time.Now()
	go KillandRestartPrimaryReplica()
	nodeEntries[3].requestForWrite(4) // node 3 request for page 4.
	time.Sleep(5 * time.Second)

	startTime = time.Now()
	go KillandRestartPrimaryReplica()
	nodeEntries[5].requestForWrite(6) // node 5 request for page 6.
	time.Sleep(6 * time.Second)

}
