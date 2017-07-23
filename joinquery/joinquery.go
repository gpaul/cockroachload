package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
)

func allowGroupAccessToResource(db *sql.DB, groupid, resourceid int) error {
	return crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		rid := strconv.Itoa(resourceid)
		gid := strconv.Itoa(groupid)
		_, err := tx.Exec("INSERT INTO aces (user_id, group_id, resource_id, actions) VALUES ($1, $2, $3, $4)",
			nil, gid, rid, "create,read,update,delete")
		return err
	})
}

func performQueries(db *sql.DB) error {
	t := time.Now()
	for ii := 0; ; ii++ {
		rows, err := db.Query("SELECT users.uid as uid from users")
		if err != nil {
			return err
		}
		userids := []string{}
		for rows.Next() {
			var userid string
			if err := rows.Scan(&userid); err != nil {
				return err
			}
			userids = append(userids, userid)
		}
		if rows.Err() != nil {
			return err
		}
		if len(userids) == 0 {
			continue
		}
		userid := userids[rand.Intn(len(userids))]
		rows, err = db.Query("SELECT resources.id AS resources_id, resources.rid AS resources_rid, resources.description AS resources_description, aces.actions AS aces_actions, aces.id AS aces_id, aces.user_id AS aces_user_id, aces.group_id AS aces_group_id, aces.resource_id AS aces_resource_id FROM aces JOIN users ON users.id = aces.user_id JOIN resources ON resources.id = aces.resource_id WHERE users.uid = $1", userid)
		if err != nil {
			return err
		}
		for rows.Next() {
			// we ignore the actual data but iterate over the rows to make sure we pull all results from the database.
		}
		if err := rows.Err(); err != nil {
			return err
		}
		now := time.Now()
		elapsed := now.Sub(t)
		t = now
		log.Printf("Query %d took %s\n", ii+1, elapsed)
	}
}

func main() {
	var addrF string
	var tlsKeyFileF string
	var tlsCertFileF string
	var tlsCACertFileF string
	flag.StringVar(&addrF, "addr", "localhost:26257", "the address of the cockroachdb instance to connect to")
	flag.StringVar(&tlsKeyFileF, "tls-key-file", "", "the path to the root user TLS key to use, if any")
	flag.StringVar(&tlsCertFileF, "tls-cert-file", "", "the path to the root user TLS certificate to use, if any")
	flag.StringVar(&tlsCACertFileF, "tls-ca-cert-file", "", "the path to the CA certificate to use, if any")
	flag.Parse()

	log.Println("Connecting to cockroachdb server")
	sslstr := "sslmode=disable"
	if tlsKeyFileF != "" {
		sslargs := []string{"sslmode=verify-full"}
		sslargs = append(sslargs, "sslrootcert="+tlsCACertFileF)
		sslargs = append(sslargs, "sslkey="+tlsKeyFileF)
		sslargs = append(sslargs, "sslcert="+tlsCertFileF)
		sslstr = strings.Join(sslargs, "&")
	}
	db, err := sql.Open("postgres", fmt.Sprintf("postgresql://root@%s/testdb?%s", addrF, sslstr))
	if err != nil {
		log.Fatal("error connecting to the database: ", err)
	}
	log.Println("Querying database")
	if err := performQueries(db); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Success")
}
