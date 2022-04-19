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
var primaryReplicaId int
var writeFile *os.File
var startTime time.Time
var lastMachine = 0
var wg = &sync.WaitGroup{}

func (c *CentralManager) listen() {
	for {
		select {

		case msg := <-c.messageChan: //Ivy protocol
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

func (c *CentralManager) onReadRequest(msg Message) {
	currentPageHolder := c.pageRecord[msg.page.id]
	currentPageHolder.messageChan <- Message{
		requestType: READ_FOWARD,
		page:        msg.page,
		senderId:    msg.senderId,
		receiverId:  currentPageHolder.id,
	}
}

func (c *CentralManager) isQueueEmpty(page Page) bool {
	defer c.pageLock[page.id].Unlock()
	c.pageLock[page.id].Lock()
	return (len(c.pageQueue[page.id]) == 0)
}

func (c *CentralManager) getQueueLength(page Page) int {
	defer c.pageLock[page.id].Unlock()
	c.pageLock[page.id].Lock()
	return len(c.pageQueue[page.id])
}

func (c *CentralManager) addQueue(msg Message) {
	defer c.pageLock[msg.page.id].Unlock()
	c.pageLock[msg.page.id].Lock()
	c.pageQueue[msg.page.id] = append(c.pageQueue[msg.page.id], msg)
}

func (c *CentralManager) PopQueue(page Page) Message {
	defer c.pageLock[page.id].Unlock()
	c.pageLock[page.id].Lock()
	msg := c.pageQueue[page.id][0]
	c.pageQueue[page.id] = c.pageQueue[page.id][1:]
	return msg
}

func (c *CentralManager) onReadConfirmation(msg Message) {
	fmt.Printf("Central Manager: Read from machine %v finished.\n", msg.senderId)
	executeNextLoop(true)
}

func (c *CentralManager) onWriteRequest(msg Message) {
	c.addQueue(msg)
	if c.getQueueLength(msg.page) == 1 {
		for _, node := range nodeEntries {
			if node.id != msg.senderId && node.id != c.pageRecord[msg.page.id].id {
				fmt.Printf("Node ID:%v \nPageRecordId:%v\n", node.id, c.pageRecord[msg.page.id].id)
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

func (c *CentralManager) onInvalidateConfirm(msg Message) {
	fmt.Printf("Node %v - invalidated page %v\n", msg.senderId, msg.page.id)
}

func (c *CentralManager) onWriteConfirmation(msg Message) {
	//update ownership record
	c.pageLock[msg.page.id].Lock()
	c.pageRecord[msg.page.id] = nodeEntries[msg.senderId]
	c.pageLock[msg.page.id].Unlock()
	fmt.Printf("Central Manager: Write from machine %v finished.\n", msg.senderId)
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

func (c *CentralManager) onCMReplicaAck(msg Message) {
	fmt.Printf("Primary CM: replica %v has updated page record\n", msg.senderId)
}

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

func (c *CentralManager) startElection() {
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

func (c *CentralManager) onCMReplyElection() {
	defer c.electionMu.Unlock()
	c.electionMu.Lock()
	c.isElected = false
}

func (c *CentralManager) onCMAnnounceCoordinator(msg Message) {
	c.primaryId = msg.senderId
}

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

func (n *Node) onSendPage(msg Message) {
	defer n.pageMu.Unlock()
	n.pageMu.Lock()
	n.pageMap[msg.page.id] = msg.page
	fmt.Printf("Node %v: Read page %v complete.\n", n.id, msg.page.id)
	cmEntries[primaryReplicaId].messageChan <- Message{
		requestType: READ_CONFIRMATION,
		page:        msg.page,
		senderId:    n.id,
		receiverId:  -1,
	}
}

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

func (n *Node) onSendPageWrite(msg Message) {
	defer n.pageMu.Unlock()
	n.pageMu.Lock()
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
func (n *Node) requestForRead(pageId int) {
	//Invalidate page
	defer n.pageMu.Unlock()
	n.pageMu.Lock()
	n.pageMap[pageId] = Page{pageId, 0}
	cmEntries[primaryReplicaId].messageChan <- Message{
		requestType: READ_REQUEST,
		page:        Page{id: pageId, value: 0},
		senderId:    n.id,
		receiverId:  -1,
	}
}

func (n *Node) requestForWrite(pageId int) {
	defer n.pageMu.Unlock()
	n.pageMu.Lock()
	fmt.Printf("Node %v - requesting for page %v write\n", n.id, pageId)
	n.pageMap[pageId] = Page{pageId, 0}
	cmEntries[primaryReplicaId].messageChan <- Message{
		requestType: WRITE_REQUEST,
		page:        Page{pageId, 0},
		senderId:    n.id,
		receiverId:  -1,
	}
}

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
		})
		go cmEntries[i].listen()
	}
}

func executeNextLoop(isRead bool) {
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
