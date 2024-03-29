package Ivy

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
	id          int // id = -1
	messageChan chan Message
	pageRecord  map[int]*Node
	copySet     map[int][]*Node
	pageQueue   map[int]([]Message)
	pageLock    map[int]*sync.Mutex
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
}

const (
	NUMBER_OF_NODES = 10

	READ_REQUEST      = 0
	READ_FOWARD       = 1
	SEND_PAGE_READ    = 2
	READ_CONFIRMATION = 3

	WRITE_REQUEST      = 4
	INVALIDATE_PAGE    = 5
	INVALIDATE_CONFIRM = 6
	WRITE_FORWARD      = 7
	SEND_PAGE_WRITE    = 8
	WRITE_CONFIRMATION = 9
)

var nodeEntries []*Node
var writeFile *os.File
var startTime time.Time
var centralManager *CentralManager
var lastMachine = 0
var wg = &sync.WaitGroup{}

/*
This is run as a go routine to listen for incoming messages.
*/
func (c *CentralManager) listen() {
	for {
		msg := <-c.messageChan

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
	fmt.Printf("Central Manager: Read from machine %v finished.\n", msg.senderId)
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
	fmt.Printf("Central Manager: Write from machine %v finished.\n", msg.senderId)
	c.PopQueue(msg.page)

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
Once the node receives the page from page holder, update local copy.
*/
func (n *Node) onSendPage(msg Message) {
	defer n.pageMu.Unlock()
	n.pageMu.Lock()
	n.pageMap[msg.page.id] = msg.page
	fmt.Printf("Node %v: Read page %v complete.\n", n.id, msg.page.id)
	centralManager.messageChan <- Message{
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
	centralManager.messageChan <- Message{
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
Once the node receives the latest page, increment its value by 1 to signify writing.
*/
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

	centralManager.messageChan <- Message{
		requestType: WRITE_CONFIRMATION,
		page:        page,
		senderId:    n.id,
		receiverId:  -1,
	}
}

/*
The node first invalidates its own local copy before sending a read request to the central manager.
*/
func (n *Node) requestForRead(pageId int) {
	//Invalidate page
	defer n.pageMu.Unlock()
	n.pageMu.Lock()
	n.pageMap[pageId] = Page{pageId, 0}
	centralManager.messageChan <- Message{
		requestType: READ_REQUEST,
		page:        Page{id: pageId, value: 0},
		senderId:    n.id,
		receiverId:  -1,
	}
}

/*
The node first invalidates its own local copy before sending a write request to the central manager.
*/
func (n *Node) requestForWrite(pageId int) {
	defer n.pageMu.Unlock()
	n.pageMu.Lock()
	fmt.Printf("Node %v - requesting for page %v write\n", n.id, pageId)
	n.pageMap[pageId] = Page{pageId, 0}
	centralManager.messageChan <- Message{
		requestType: WRITE_REQUEST,
		page:        Page{pageId, 0},
		senderId:    n.id,
		receiverId:  -1,
	}
}

/*
Initialises with Ivy architecture with NUMBER_OF_NODES.
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

	centralManager = &CentralManager{
		id:          -1,
		messageChan: make(chan Message, 100),
		pageRecord:  pageRecord,
		pageQueue:   pageQueue,
		pageLock:    pageLock,
	}
	go centralManager.listen()
}

/*
Helper function to chain requests. This also helps to record time taken for a request to complete.
*/
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

/*
Experiment 1. We perform 9 read requests one at a time, and 9 write requests one at a time. The results are written into the ivy_timings.txt file.
*/
func Experiment1() {
	initialise()
	writeFile, _ = os.Create("./Pset/Pset3/Experiment1/ivy_timings.txt")
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
