package main

import (
    "fmt"
    "time"
)

func main() {
    temp := 0
    t1 := time.Now()
    for i := 0; i < 100000; i++ {
        for j := 0; j < 10000; j++ {
            temp++
        }
    }
    fmt.Println(temp)
    t2 := time.Now()
    fmt.Println(t2.Sub(t1))

}