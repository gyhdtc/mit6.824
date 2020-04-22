package main

import (
	"fmt"
	"time"
)

type shit struct {
	flag int
	timeout1 chan bool
	timeout2 chan bool
}

func (s *shit) gyh() {
	for i := 0; i < 5; i ++ {
		go func(i int) {
			
			select {
			case <- time.Tick(time.Millisecond * time.Duration(i * 200 + 200)):
				fmt.Printf("time 1 - %d\n", i)
			case <- s.timeout2:
				fmt.Printf("time 2 - %d\n", i)
			}
		}(i)
	}
}

func main() {
	s1 := shit{}
	s1.timeout1 = make(chan bool, 5)
	s1.timeout2 = make(chan bool, 5)

	s1.gyh()
	time.Sleep(500 * time.Millisecond)
	// for i := 0; i < 5; i++ {
	// 	s1.timeout2 <- true
	// }
	i := 0
	for i == 0 {
		select {
		case <- time.Tick(time.Millisecond * time.Duration(2000)):
			i = 1
		case s1.timeout2 <- true:
		}
	}
	fmt.Printf("end1")
	//x := <- s1.timeout1
	//fmt.Printf("time 1 - %s\n", x)
	time.Sleep(1000 * time.Millisecond)
	fmt.Printf("end2")
}