package main

import (
	"flag"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"strings"
	"sync"
	"time"
)

// conenct zookeeper cluster
func connect_zk() (*zk.Conn, error) {
	c, _, err := zk.Connect([]string{"10.15.2.112:2181"}, time.Second*1)
	// c, session, err := zk.Connect([]string{"127.0.0.1:2182"}, time.Second*1)
	if err != nil {
		return nil, err
	}
	return c, nil
}

//获得节点下面的子节点
func children_node(c *zk.Conn, path string) []string {
	str, _, err := c.Children(path)

	if err == nil {
		for _, v := range str {
			fmt.Println(v + "\n")
		}
	}

	return str
}

func delete_node(c *zk.Conn, nodePath string) {

	children := children_node(c, nodePath)

	if children != nil {
		for _, v := range children {
			delete_node(c, nodePath+"/"+v)
		}
	}

	err := c.Delete(nodePath, -1)

	if err != nil {
		fmt.Println("delete node is failed,reason:", err)
	} else {
		fmt.Println("delete node success")
	}
}

func create_node(c *zk.Conn, nodePath string) error {
	if _, err := c.Create(nodePath, []byte{1, 2, 3, 4}, 0, zk.WorldACL(zk.PermAll)); err != nil {
		fmt.Printf("Create returned error: %+v\n", err)
		return err
	}
	return nil
}

func get_node(c *zk.Conn, path string) {
	data, stat, err := c.Get(path)
	if err != nil {
		fmt.Printf("Get returned error: %+v\n", err)
	} else {
		str := string(data)

		newStr := strings.Replace(str, ",", "\n", -1)
		fmt.Printf("Get node, data: %s, state: %+v\n", newStr, stat)
	}
}

func watch_node(c *zk.Conn) {
	var wg sync.WaitGroup
	path := "/gozk-test-2"

	delete_node(c, path)

	_, _, childCh, _ := c.ChildrenW("/")
	/*
	   if err != nil {
	       fmt.Printf("Watch error: %v\n", err)
	       return
	   }
	*/

	wg.Add(1)
	go Watcher(wg, childCh)

	create_node(c, path)

	/*
	   if path, err = c.Create("/gozk-test-2", []byte{1, 2, 3, 4}, 0, zk.WorldACL(zk.PermAll)); err != nil {
	       fmt.Printf("Creat node: %v error: %v\n", "/gozk-test-2", err)
	       return
	   } else if path != "/gozk-test-2" {
	       fmt.Printf("Create returned different path '%s' != '%s'", path, "/gozk-test-2")
	       return
	   }
	*/

	// add watcher for new added node
	_, _, addCh, _ := c.ChildrenW("/gozk-test-2")
	wg.Add(1)
	go Watcher(wg, addCh)

	delete_node(c, path)

	wg.Wait()
}

func Watcher(wg sync.WaitGroup, childCh <-chan zk.Event) {
	defer wg.Done()
	select {
	case ev := <-childCh:
		if ev.Err != nil {
			fmt.Printf("Child watcher error: %+v\n", ev.Err)
			return
		}
		fmt.Printf("Receive event: %+v\n", ev)
	case _ = <-time.After(time.Second * 2):
		fmt.Printf("Child Watcher timeout")
		return
	}
}

func main() {
	option := flag.String("op", "get", "读取操作")
	path := flag.String("p", "/pigeon", "具体某一个节点的路径")
	child := flag.Bool("c", false, "读取子节点")

	//option:get,create,delete
	flag.Parse()
	// connect zk-server
	c, err := connect_zk()
	if err != nil {
		return
	}

	switch *option {
	case "get":
		if *child {
			children_node(c, *path)
		} else {
			get_node(c, *path)
		}
	case "create":
		create_node(c, *path)
	case "delete":
		delete_node(c, *path)
	}
}
