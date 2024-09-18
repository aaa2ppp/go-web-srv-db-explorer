// тут лежит тестовый код
// менять вам может потребоваться только коннект к базе
package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

var (
	// DSN это соединение с базой
	// вы можете изменить этот на тот который вам нужен
	// docker run -p 3306:3306 -v $(PWD):/docker-entrypoint-initdb.d -e MYSQL_ROOT_PASSWORD=1234 -e MYSQL_DATABASE=golang -d mysql
	// DSN = "root@tcp(localhost:3306)/golang2017?charset=utf8"
	// DSN = "coursera:5QPbAUufx7@tcp(localhost:3306)/coursera?charset=utf8"
	DSN = "%s:%s@tcp(%s:%s)/%s?charset=utf8"
)

func init() {
	set := func(p *string, env string, defVal string, req bool) {
		if v, ok := os.LookupEnv(env); ok {
			*p = v
		} else if req {
			log.Fatalf("env %s is required", env)
		} else {
			*p = defVal
		}
	}

	var host, port, base, user, pass string
	set(&host, "DB_HOST", "localhost", false)
	set(&port, "SB_PORT", "3306", false)
	set(&base, "DB_BASE", "", true)
	set(&user, "DB_USER", "root", false)
	set(&pass, "DB_PASS", "", true)

	DSN = fmt.Sprintf(DSN, user, pass, host, port, base)
}

func main() {
	db, err := sql.Open("mysql", DSN)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping() // вот тут будет первое подключение к базе
	if err != nil {
		log.Fatal(err)
	}

	handler, err := NewDbExplorer(db)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("starting server at :8082")
	log.Fatal(http.ListenAndServe(":8082", handler))
}
