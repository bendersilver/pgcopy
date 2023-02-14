package main

import (
	"log"
	"os"

	"github.com/bendersilver/pgcopy"
)

func main() {
	_, err := pgcopy.New(os.Getenv("PG_URL"), "pb", "employee")
	log.Fatal(err)
}
