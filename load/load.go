package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
)

const schema = `
DROP DATABASE IF EXISTS testdb;
CREATE DATABASE testdb;
SET DATABASE=testdb;

CREATE TABLE aces (
	id INTEGER NOT NULL DEFAULT unique_rowid(),
	user_id INTEGER NULL,
	group_id INTEGER NULL,
	resource_id INTEGER NULL,
	actions STRING NULL,
	CONSTRAINT "primary" PRIMARY KEY (id ASC),
	CONSTRAINT user_resource_unique UNIQUE (user_id, resource_id),
	CONSTRAINT group_resource_unique UNIQUE (group_id, resource_id),
	FAMILY "primary" (id, user_id, group_id, resource_id, actions)
);

CREATE TABLE configs (
	id INTEGER NOT NULL DEFAULT unique_rowid(),
	key STRING NULL,
	value STRING NULL,
	CONSTRAINT "primary" PRIMARY KEY (id ASC),
	UNIQUE INDEX configs_key_key (key ASC),
	FAMILY "primary" (id, key, value)
);

CREATE TABLE groups (
	id INTEGER NOT NULL DEFAULT unique_rowid(),
	gid STRING NULL,
	description STRING NULL,
	CONSTRAINT "primary" PRIMARY KEY (id ASC),
	UNIQUE INDEX groups_gid_key (gid ASC),
	FAMILY "primary" (id, gid, description)
);

CREATE TABLE resources (
	id INTEGER NOT NULL DEFAULT unique_rowid(),
	rid STRING NULL,
	description STRING NULL,
	CONSTRAINT "primary" PRIMARY KEY (id ASC),
	UNIQUE INDEX resources_rid_key (rid ASC),
	FAMILY "primary" (id, rid, description)
);

CREATE TABLE user_groups (
	user_id INTEGER NOT NULL,
	group_id INTEGER NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (user_id ASC, group_id ASC),
	UNIQUE INDEX user_groups_user_id_group_id_key (user_id ASC, group_id ASC),
	FAMILY "primary" (user_id, group_id)
);

CREATE TABLE users (
	id INTEGER NOT NULL DEFAULT unique_rowid(),
	uid STRING NULL,
	passwordhash STRING NULL,
	utype STRING(7) NULL,
	description STRING NULL,
	is_remote BOOL NULL,
	CONSTRAINT "primary" PRIMARY KEY (id ASC),
	UNIQUE INDEX users_uid_key (uid ASC),
	FAMILY "primary" (id, uid, passwordhash, utype, description, is_remote),
	CONSTRAINT usertype CHECK (utype IN ('regular':::STRING, 'service':::STRING))
);
`

var verbose bool

func main() {
	var (
		addrF             string
		tlsKeyFileF       string
		tlsCertFileF      string
		tlsCACertFileF    string
		customF           bool
		usersF            int
		groupsF           int
		membersF          int
		userPermissionsF  int
		groupPermissionsF int
		verboseF          bool
	)
	flag.StringVar(&addrF, "addr", "localhost:26257", "the address of the cockroachdb instance to connect to")
	flag.StringVar(&tlsKeyFileF, "tls-key-file", "", "the path to the root user TLS key to use, if any")
	flag.StringVar(&tlsCertFileF, "tls-cert-file", "", "the path to the root user TLS certificate to use, if any")
	flag.StringVar(&tlsCACertFileF, "tls-ca-cert-file", "", "the path to the CA certificate to use, if any")
	flag.BoolVar(&customF, "custom", false, "use custom provided record counts")
	flag.IntVar(&usersF, "users", 0, "number of users (use with -custom)")
	flag.IntVar(&groupsF, "groups", 0, "number of groups (use with -custom)")
	flag.IntVar(&membersF, "members", 0, "number of members per group (use with -custom)")
	flag.IntVar(&userPermissionsF, "user-permissions", 0, "number of permissions per user (use with -custom)")
	flag.IntVar(&groupPermissionsF, "group-permissions", 0, "number of permissions per group (use with -custom)")
	flag.BoolVar(&verboseF, "verbose", false, "print detailed timing data")
	flag.Parse()

	if verboseF {
		say("enabled verbose logging")
		verbose = verboseF
	}

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

	if err := logTiming("Creating database and schema", func() error {
		return crdb.ExecuteTx(db, createSchema)
	}); err != nil {
		log.Fatal(err)
	}

	if customF {
		var counts recordCount
		counts[Users] = usersF
		counts[Groups] = groupsF
		counts[Members] = membersF
		counts[UserPermissions] = userPermissionsF
		counts[GroupPermissions] = groupPermissionsF
		if err := logTiming("Loading data", func() error {
			return runWithCounts(db, counts)
		}); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := logTiming("Loading data", func() error {
		return run(db)
	}); err != nil {
		log.Fatal(err)
	}
}

var logdepth int

func logTiming(msg string, fn func() error) error {
	logdepth++
	defer func() { logdepth-- }()
	if verbose {
		say("%s ... starting", msg)
	}
	t := time.Now()
	if err := fn(); err != nil {
		say("%s ... failed (%s): err=%v", msg, time.Since(t), err)
		return err
	}
	elapsed := time.Now().Sub(t)
	say("%s ... done (%s)", msg, elapsed)
	return nil
}

func logTimingV(msg string, fn func() error) error {
	if verbose {
		return logTiming(msg, fn)
	}
	return fn()
}

func say(msg string, args ...interface{}) {
	msg = logprefix(msg)
	log.Printf(msg, args...)
}

func logprefix(msg string) string {
	return strings.Repeat("  ", logdepth) + msg
}

func createSchema(tx *sql.Tx) error {
	_, err := tx.Exec(schema)
	return err
}

func run(db *sql.DB) error {
	for iteration := 0; ; iteration++ {
		counts := recordCountForIteration(iteration)
		msg := fmt.Sprintf("Iteration %d (%s)", iteration, counts)
		if err := logTiming(msg, func() error {
			return runWithCounts(db, counts)
		}); err != nil {
			return err
		}
	}
	return nil
}

func runWithCounts(db *sql.DB, counts recordCount) error {
	if !counts.sane() {
		say("Skipping non-sensical data mixture: %s", counts)
		return nil
	}
	defer func() {
		if cerr := logTimingV("Removing data", func() error {
			return removeData(db)
		}); cerr != nil {
			panic(cerr)
		}
	}()
	return prepareData(db, counts)
}

type RecordType uint

const (
	Users RecordType = iota
	Groups
	Members
	UserPermissions
	GroupPermissions
	LastRecordType = iota
)

type recordCount [LastRecordType]int

func (counts recordCount) sane() bool {
	if counts[Members] > 0 && counts[Groups] == 0 {
		return false
	}
	if counts[Members] > 0 && counts[Users] == 0 {
		return false
	}
	if counts[Members] > counts[Users] {
		return false
	}
	if counts[UserPermissions] > 0 && counts[Users] == 0 {
		return false
	}
	if counts[GroupPermissions] > 0 && counts[Groups] == 0 {
		return false
	}
	return true
}

func (counts recordCount) String() string {
	return fmt.Sprintf("{users: %d, groups: %d, members: %d, user-permissions: %d, group-permissions: %d}",
		counts[Users], counts[Groups], counts[Members], counts[UserPermissions], counts[GroupPermissions])
}

// recordCountForIteration returns an array of elements, one for each RecordType, specifying how many records of that type to generate.
// It does so by treating the returned array as a binary string, where a '1' means 'generate records for this RecordType' and '0' means
// 'don't increment the number of records of this RecordType'.
// An overflow is treated as "increment all record counts.
func recordCountForIteration(iteration int) (counts recordCount) {
	baseline := iteration >> LastRecordType
	if baseline == iteration {
		// This overflow scenario was already handled by the last iteration
		return counts
	}
	for cidx := range counts {
		counts[cidx] = baseline * 20
	}
	for rt := RecordType(0); rt < LastRecordType; rt++ {
		if iteration&(1<<rt) != 0 {
			counts[rt] += 20
		}
	}
	return counts
}

// prepareData adds records singly, to simulate performing such a task through a non-bulk interface
func prepareData(db *sql.DB, counts recordCount) error {
	if !counts.sane() {
		panic("prepareData: recordCount is not sane")
	}
	if err := logTimingV("Add users", func() error {
		return addUsers(db, counts[Users])
	}); err != nil {
		return err
	}
	if err := logTimingV("Add groups", func() error {
		return addGroups(db, counts[Groups])
	}); err != nil {
		return err
	}
	if err := logTimingV("Assign users to groups", func() error {
		return assignUsersToGroups(db, counts[Members], counts[Groups], counts[Users])
	}); err != nil {
		return err
	}
	if err := logTimingV("Assign user permissions", func() error {
		return assignUserPermissions(db, counts[UserPermissions], counts[Users])
	}); err != nil {
		return err
	}
	if err := logTimingV("Assign group permissions", func() error {
		return assignGroupPermissions(db, counts[GroupPermissions], counts[Groups])
	}); err != nil {
		return err
	}
	return nil
}

func addUsers(db *sql.DB, users int) error {
	for ii := 0; ii < users; ii++ {
		if err := logTimingV(fmt.Sprintf("Add user %d", ii), func() error {
			return addUser(db, ii)
		}); err != nil {
			return err
		}
	}
	return nil
}

func addUser(db *sql.DB, userid int) error {
	return crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		uid := strconv.Itoa(userid)
		passwordhash := "$6$rounds=656000$WZdTPdpxUZsDG5PG$6om6ApIm5l5639JNAUmtFD87cIXdWCAVKeJ4zNlhmPKWT3PARF6Ai.HpcjR8SPQSQnqoefBiLaZmPuMFhGhpm0"
		utype := "regular"
		description := "some description"
		isRemote := false
		_, err := tx.Exec("INSERT INTO users (uid, passwordhash, utype, description, is_remote) VALUES ($1, $2, $3, $4, $5) RETURNING users.id",
			uid, passwordhash, utype, description, isRemote)
		return err
	})
}

func addGroups(db *sql.DB, groups int) error {
	for ii := 0; ii < groups; ii++ {
		if err := logTimingV(fmt.Sprintf("Add group %d", ii), func() error {
			return addGroup(db, ii)
		}); err != nil {
			return err
		}
	}
	return nil
}

func addGroup(db *sql.DB, groupid int) error {
	return crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		gid := strconv.Itoa(groupid)
		description := "some description"
		_, err := tx.Exec("INSERT INTO groups (gid, description) VALUES ($1, $2) RETURNING groups.id",
			gid, description)
		return err
	})
}

func assignUsersToGroups(db *sql.DB, members, groups, users int) error {
	user := 0
	for group := 0; group < groups; group++ {
		for ii := 0; ii < members; ii++ {
			if err := logTimingV(fmt.Sprintf("Add user %d to group %d", user, group), func() error {
				return addUserToGroup(db, group, user)
			}); err != nil {
				return err
			}
			user++
			user = user % users
		}
	}
	return nil
}

func addUserToGroup(db *sql.DB, group, user int) error {
	return crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		var userId int
		row := tx.QueryRow("SELECT id from users where users.uid LIKE $1", strconv.Itoa(user))
		if err := row.Scan(&userId); err != nil {
			return err
		}
		var groupId int
		row = tx.QueryRow("SELECT id from groups where groups.gid LIKE $1", strconv.Itoa(group))
		if err := row.Scan(&groupId); err != nil {
			return err
		}
		_, err := tx.Exec("INSERT INTO user_groups (user_id, group_id) VALUES ($1, $2)",
			userId, groupId)
		return err
	})
}

func assignUserPermissions(db *sql.DB, permissions, users int) error {
	for permission := 0; permission < permissions; permission++ {
		resource := userResourceName(permission)
		if err := logTimingV(fmt.Sprintf("Add resource %s", resource), func() error {
			return addResource(db, resource)
		}); err != nil {
			return err
		}
		for user := 0; user < users; user++ {
			if err := logTimingV(fmt.Sprintf("Allow %s to user %d", resource, user), func() error {
				return allowUserAccessToResource(db, resource, user)
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func userResourceName(rid int) string { return "user-resource-" + strconv.Itoa(rid) }

func allowUserAccessToResource(db *sql.DB, resource string, uid int) error {
	for _, action := range []string{"create", "read", "update", "delete"} {
		if err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
			return logTimingV("inside", func() error {
				var resourceId int64
				if err := logTimingV("find resource "+resource, func() error {
					row := tx.QueryRow("SELECT resources.id as id from resources where resources.rid LIKE $1", resource)
					return row.Scan(&resourceId)
				}); err != nil {
					return err
				}
				var userId int64
				if err := logTimingV("find user "+strconv.Itoa(uid), func() error {
					row := tx.QueryRow("SELECT users.id as id from users where users.uid LIKE $1", strconv.Itoa(uid))
					return row.Scan(&userId)
				}); err != nil {
					return err
				}
				var actionstr string
				var aceId string
				if err := logTimingV("find ace", func() error {
					row := tx.QueryRow("SELECT aces.actions as actions, aces.id as id from aces where aces.user_id = $1 and aces.resource_id = $2", userId, resourceId)
					return row.Scan(&actionstr, &aceId)
				}); err != nil && err != sql.ErrNoRows {
					return err
				}
				if len(actionstr) > 0 {
					actionstr += "," + action
					if err := logTimingV("update ace actions="+actionstr, func() error {
						_, err := tx.Exec("UPDATE aces SET actions = $1 WHERE aces.id=$2",
							actionstr, aceId)
						return err
					}); err != nil {
						return err
					}
				} else {
					actionstr = action
					if err := logTimingV("insert ace actions="+actionstr, func() error {
						_, err := tx.Exec("INSERT INTO aces (user_id, group_id, resource_id, actions) VALUES ($1, $2, $3, $4)",
							userId, nil, resourceId, actionstr)
						return err
					}); err != nil {
						return err
					}
				}
				return nil
			})
		}); err != nil {
			return err
		}
	}
	return nil
}

func assignGroupPermissions(db *sql.DB, permissions, groups int) error {
	for permission := 0; permission < permissions; permission++ {
		resource := groupResourceName(permission)
		if err := logTimingV(fmt.Sprintf("Add resource %s", resource), func() error {
			return addResource(db, resource)
		}); err != nil {
			return err
		}
		for group := 0; group < groups; group++ {
			if err := logTimingV(fmt.Sprintf("Allow %s to group %d", resource, group), func() error {
				return allowGroupAccessToResource(db, resource, group)
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func groupResourceName(rid int) string { return "group-resource-" + strconv.Itoa(rid) }

func allowGroupAccessToResource(db *sql.DB, resource string, gid int) error {
	for _, action := range []string{"create", "read", "update", "delete"} {
		if err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
			row := tx.QueryRow("SELECT resources.id as id from resources where resources.rid LIKE $1", resource)
			var resourceId int64
			if err := row.Scan(&resourceId); err != nil {
				return err
			}
			row = tx.QueryRow("SELECT groups.id as id from groups where groups.gid LIKE $1", strconv.Itoa(gid))
			var groupId int64
			if err := row.Scan(&groupId); err != nil {
				return err
			}
			row = tx.QueryRow("SELECT aces.actions as actions, aces.id as id from aces where aces.group_id = $1 and aces.resource_id = $2", groupId, resourceId)
			var actionstr string
			var aceId int64
			err := row.Scan(&actionstr, &aceId)
			if err != nil && err != sql.ErrNoRows {
				return err
			}
			if len(actionstr) > 0 {
				actionstr += "," + action
				if _, err := tx.Exec("UPDATE aces SET actions = $1 WHERE aces.id=$2",
					actionstr, aceId); err != nil {
					return err
				}
			} else {
				actionstr = action
				if _, err := tx.Exec("INSERT INTO aces (user_id, group_id, resource_id, actions) VALUES ($1, $2, $3, $4)",
					nil, groupId, resourceId, actionstr); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func addResource(db *sql.DB, resource string) error {
	return crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		description := "some description"
		_, err := tx.Exec("INSERT INTO resources (rid, description) VALUES ($1, $2) RETURNING resources.id",
			resource, description)
		return err
	})
}

// removeData removes records singly, to simulate performing such a task through a non-bulk interface
func removeData(db *sql.DB) error {
	if err := logTimingV("Remove users", func() error {
		return removeUsers(db)
	}); err != nil {
		return err
	}
	if err := logTimingV("Remove groups", func() error {
		return removeGroups(db)
	}); err != nil {
		return err
	}
	if err := logTimingV("Remove resources", func() error {
		return removeResources(db)
	}); err != nil {
		return err
	}
	return nil
}

func removeUsers(db *sql.DB) error {
	uids, err := findUsers(db)
	if err != nil {
		return err
	}
	for _, uid := range uids {
		if err := logTimingV(fmt.Sprintf("Remove user %s", uid), func() error {
			return removeUser(db, uid)
		}); err != nil {
			return err
		}
	}
	return nil
}

func findUsers(db *sql.DB) (uids []string, err error) {
	if err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		rows, err := tx.Query("SELECT uid from users")
		if err != nil {
			return err
		}
		for rows.Next() {
			var uid string
			if err := rows.Scan(&uid); err != nil {
				return err
			}
			uids = append(uids, uid)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return uids, nil
}

func removeUser(db *sql.DB, uid string) error {
	return crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		row := tx.QueryRow("SELECT id from users where uid LIKE $1", uid)
		var id int64
		if err := row.Scan(&id); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM user_groups where user_id = $1", id); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM aces where user_id = $1", id); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM users where id = $1", id); err != nil {
			return err
		}
		return nil
	})
}

func removeGroups(db *sql.DB) error {
	gids, err := findGroups(db)
	if err != nil {
		return err
	}
	for _, gid := range gids {
		if err := logTimingV(fmt.Sprintf("Remove group %s", gid), func() error {
			return removeGroup(db, gid)
		}); err != nil {
			return err
		}
	}
	return nil
}

func findGroups(db *sql.DB) (gids []string, err error) {
	if err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		rows, err := tx.Query("SELECT gid from groups")
		if err != nil {
			return err
		}
		for rows.Next() {
			var gid string
			if err := rows.Scan(&gid); err != nil {
				return err
			}
			gids = append(gids, gid)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return gids, nil
}

func removeGroup(db *sql.DB, gid string) error {
	return crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		row := tx.QueryRow("SELECT id from groups where gid LIKE $1", gid)
		var id int64
		if err := row.Scan(&id); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM user_groups where group_id = $1", id); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM aces where group_id = $1", id); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM groups where id = $1", id); err != nil {
			return err
		}
		return nil
	})
}

func removeResources(db *sql.DB) error {
	rids, err := findResources(db)
	if err != nil {
		return err
	}
	for _, rid := range rids {
		if err := logTimingV(fmt.Sprintf("Remove resource %s", rid), func() error {
			return removeResource(db, rid)
		}); err != nil {
			return err
		}
	}
	return nil
}

func findResources(db *sql.DB) (rids []string, err error) {
	if err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		rows, err := tx.Query("SELECT rid from resources")
		if err != nil {
			return err
		}
		for rows.Next() {
			var rid string
			if err := rows.Scan(&rid); err != nil {
				return err
			}
			rids = append(rids, rid)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return rids, nil
}

func removeResource(db *sql.DB, rid string) error {
	return crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		row := tx.QueryRow("SELECT id from resources where rid LIKE $1", rid)
		var id int64
		if err := row.Scan(&id); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM aces where resource_id = $1", id); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM resources where id = $1", id); err != nil {
			return err
		}
		return nil
	})
}
