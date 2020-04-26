// Read all Completed messages from bb_ext_paypal and write them to 4priority service

package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/jmoiron/sqlx"
	_ "github.com/joho/godotenv/autoload"
	_ "github.com/pkg/errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

// Read messages from database
type Contribution struct {
	ID                string          `db:"ID"`
	ORG               string          `db:"ORG"`
	CID               sql.NullString  `db:"CID"`
	QAMO_PARTNAME     sql.NullString  `db:"QAMO_PARTNAME"`
	QAMO_VAT          sql.NullString  `db:"QAMO_VAT"`
	QAMO_CUSTDES      sql.NullString  `db:"QAMO_CUSTDES"`
	QAMO_DETAILS      int64           `db:"QAMO_DETAILS"`
	QAMO_PARTDES      sql.NullString  `db:"QAMO_PARTDES"`
	QAMO_PAYMENTCODE  sql.NullString  `db:"QAMO_PAYMENTCODE"`
	QAMO_CARDNUM      sql.NullString  `db:"QAMO_CARDNUM"`
	QAMT_AUTHNUM      sql.NullString  `db:"QAMT_AUTHNUM"`
	QAMO_PAYMENTCOUNT sql.NullString  `db:"QAMO_PAYMENTCOUNT"`
	QAMO_VALIDMONTH   sql.NullString  `db:"QAMO_VALIDMONTH"`
	QAMO_PAYPRICE     float64         `db:"QAMO_PAYPRICE"`
	QAMO_CURRNCY      sql.NullString  `db:"QAMO_CURRNCY"`
	QAMO_PAYCODE      sql.NullInt64   `db:"QAMO_PAYCODE"`
	QAMO_FIRSTPAY     sql.NullFloat64 `db:"QAMO_FIRSTPAY"`
	QAMO_EMAIL        sql.NullString  `db:"QAMO_EMAIL"`
	QAMO_ADRESS       sql.NullString  `db:"QAMO_ADRESS"`
	QAMO_CITY         sql.NullString  `db:"QAMO_CITY"`
	QAMO_CELL         sql.NullString  `db:"QAMO_CELL"`
	QAMO_FROM         sql.NullString  `db:"QAMO_FROM"`
	QAMM_UDATE        sql.NullString  `db:"QAMM_UDATE"`
	QAMO_LANGUAGE     sql.NullString  `db:"QAMO_LANGUAGE"`
	QAMO_REFERENCE    sql.NullString  `db:"QAMO_REFERENCE"`
}

var (
	urlStr string
	err    error
)

func main() {

	host := os.Getenv("CIVI_HOST")
	if host == "" {
		host = "localhost"
	}
	dbName := os.Getenv("CIVI_DBNAME")
	if dbName == "" {
		dbName = "localhost"
	}
	user := os.Getenv("CIVI_USER")
	if user == "" {
		log.Fatalf("Unable to connect without username\n")
	}
	password := os.Getenv("CIVI_PASSWORD")
	if password == "" {
		log.Fatalf("Unable to connect without password\n")
	}
	protocol := os.Getenv("CIVI_PROTOCOL")
	if protocol == "" {
		log.Fatalf("Unable to connect without protocol\n")
	}
	prioHost := os.Getenv("PRIO_HOST")
	if prioHost == "" {
		log.Fatalf("Unable to connect Priority without host name\n")
	}
	prioPort := os.Getenv("PRIO_PORT")
	if prioPort == "" {
		log.Fatalf("Unable to connect Priority without port number\n")
	}

	db, stmt := OpenDb(host, user, password, protocol, dbName)
	defer closeDb(db)

	urlStr = "http://" + prioHost + ":" + prioPort + "/payment_event"

	ReadMessages(db, stmt)
}

// Connect to DB
func OpenDb(host string, user string, password string, protocol string, dbName string) (db *sqlx.DB, stmt *sql.Stmt) {

	dsn := fmt.Sprintf("%s:%s@%s(%s)/%s", user, password, protocol, host, dbName)
	if db, err = sqlx.Open("mysql", dsn); err != nil {
		log.Fatalf("DB connection error: %v\n", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("DB real connection error: %v\n", err)
	}

	if !isTableExists(db, dbName, "bb_ext_paypal") {
		log.Fatalf("Table 'bb_ext_paypal' does not exist\n")
	}

	stmt, err = db.Prepare("UPDATE bb_ext_paypal SET status = 'processed' WHERE id = ?")
	if err != nil {
		log.Fatalf("Unable to prepare UPDATE statement: %v\n", err)
	}

	return
}

func closeDb(db *sqlx.DB) {
	db.Close()
}

func isTableExists(db *sqlx.DB, dbName string, tableName string) (exists bool) {
	var name string

	if err = db.QueryRow(
		"SELECT table_name name FROM information_schema.tables WHERE table_schema = '" + dbName +
			"' AND table_name = '" + tableName + "' LIMIT 1").Scan(&name); err != nil {
		return false
	} else {
		return name == tableName
	}
}

func ReadMessages(db *sqlx.DB, markAsDone *sql.Stmt) {
	totalPaymentsRead := 0
	contribution := Contribution{}
	rows, err := db.Queryx(`
SELECT
  id ID,
  reference QAMO_REFERENCE,
  organization ORG,
  sku QAMO_PARTNAME,
  '0' QAMO_VAT,
  name QAMO_CUSTDES, -- שם לקוח
  1 QAMO_DETAILS, -- participants
  SUBSTRING(details, 1, 48) QAMO_PARTDES, -- תאור מוצר
  CASE currency
	WHEN 'USD' THEN 'PPU' -- PayPal USD
	WHEN 'EUR' THEN 'PPE' -- PayPal EURO
	WHEN 'ILS' THEN 'PPS' -- PayPal SHEKEL
	ELSE 'PPX'  -- PayPal Unknown
  END QAMO_PAYMENTCODE, -- קוד אמצעי תשלום
  '' QAMO_CARDNUM,
  '' QAMT_AUTHNUM,
  '' QAMO_PAYMENTCOUNT, -- מס כרטיס/חשבון
  '' QAMO_VALIDMONTH, -- תוקף
  price QAMO_PAYPRICE, -- סכום בפועל
  CASE currency
    WHEN 'USD' THEN '$'
    WHEN 'EUR' THEN 'EUR'
    ELSE 'ש"ח'
  END QAMO_CURRNCY, -- קוד מטבע
  1 QAMO_PAYCODE, -- קוד תנאי תשלום
  0 QAMO_FIRSTPAY, -- גובה תשלום ראשון
  email QAMO_EMAIL, -- אי מייל
  street QAMO_ADRESS, -- כתובת
  city QAMO_CITY, -- עיר
  phone QAMO_CELL, -- נייד
  country QAMO_FROM, -- מקור הגעה (country)
  created_at QAMM_UDATE,
  CASE language
  	WHEN 'HE' THEN 'HE' 
  	WHEN 'RU' THEN 'RU' 
    ELSE 'EN' 
  END QAMO_LANGUAGE
FROM bb_ext_paypal
WHERE
  status = 'new'
	`)
	if err != nil {
		log.Fatalf("Unable to select rows: %v\n", err)
	}

	for rows.Next() {
		// Read messages from DB
		err = rows.StructScan(&contribution)
		if err != nil {
			log.Fatalf("Table 'bb_ext_paypal' access error: %v\n", err)
		}
		// Submit 2 priority
		submit2priority(contribution)

		// Update Reported2prio in case of success
		updateReported2prio(markAsDone, contribution.ID)
		totalPaymentsRead++
	}

	fmt.Printf("Total of %d payments were transferred to Priority\n", totalPaymentsRead)
}

func submit2priority(contribution Contribution) {
	// priority's database structure
	type Priority struct {
		ID           string  `json:"id"`
		UserName     string  `json:"name"`
		Amount       float64 `json:"amount"`
		Currency     string  `json:"currency"`
		Email        string  `json:"email"`
		Phone        string  `json:"phone"`
		Address      string  `json:"address"`
		City         string  `json:"city"`
		Country      string  `json:"country"`
		Description  string  `json:"event"`
		Participants int64   `json:"participants"`
		Income       string  `json:"income"`
		Is46         bool    `json:"is46"`
		Token        string  `json:"token"`
		Approval     string  `json:"approval"`
		CardType     string  `json:"cardtype"`
		CardNum      string  `json:"cardnum"`
		CardExp      string  `json:"cardexp"`
		Installments int64   `json:"installments"`
		FirstPay     float64 `json:"firstpay"`
		CreatedAt    string  `json:"created_at"`
		Language     string  `json:"language"`
		Reference    string  `json:"reference"`
		Organization string  `json:"organization"`
		IsVisual     bool    `json:"is_visual"`
	}

	type Message struct {
		Error   bool
		Message string
	}

	priority := Priority{
		ID:           contribution.ID,
		UserName:     contribution.QAMO_CUSTDES.String,
		Participants: contribution.QAMO_DETAILS,
		Income:       contribution.QAMO_PARTNAME.String,
		Description:  contribution.QAMO_PARTDES.String,
		CardType:     contribution.QAMO_PAYMENTCODE.String,
		CardNum:      contribution.QAMO_PAYMENTCOUNT.String,
		CardExp:      contribution.QAMO_VALIDMONTH.String,
		Amount:       contribution.QAMO_PAYPRICE,
		Currency:     contribution.QAMO_CURRNCY.String,
		Installments: contribution.QAMO_PAYCODE.Int64,
		FirstPay:     contribution.QAMO_FIRSTPAY.Float64,
		Token:        contribution.QAMO_CARDNUM.String,
		Approval:     contribution.QAMT_AUTHNUM.String,
		Is46:         contribution.QAMO_VAT.String == "1",
		Email:        contribution.QAMO_EMAIL.String,
		Address:      contribution.QAMO_ADRESS.String,
		City:         contribution.QAMO_CITY.String,
		Country:      contribution.QAMO_FROM.String,
		Phone:        contribution.QAMO_CELL.String,
		CreatedAt:    contribution.QAMM_UDATE.String,
		Language:     contribution.QAMO_LANGUAGE.String,
		Reference:    contribution.QAMO_REFERENCE.String,
		Organization: contribution.ORG,
		IsVisual:     false, // CiviCRM produces Logical Hebrew
	}

	marshal, err := json.Marshal(priority)
	if err != nil {
		log.Fatalf("Marshal error: %v\n", err)
	}
	log.Printf("=> %s\n", marshal)
	log.Printf("=> %s\n", urlStr)

	req, err := http.NewRequest("POST", urlStr, bytes.NewBuffer(marshal))
	if err != nil {
		log.Fatalf("NewRequest error: %v\n", err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("client.Do error: %v\n", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("ReadAll error: %v\n", err)
	}
	message := Message{}
	log.Printf("<= %s\n", body)
	if err := json.Unmarshal(body, &message); err != nil {
		log.Fatalf("Unmarshal error: %v\n", err)
	}
	if message.Error {
		log.Fatalf("Response error: %s\n", message.Message)
	}
}

func updateReported2prio(stmt *sql.Stmt, id string) {
	res, err := stmt.Exec(id)
	if err != nil {
		log.Fatalf("Update error: %v\n", err)
	}
	rowCnt, err := res.RowsAffected()
	if err != nil {
		log.Fatalf("Update error: %v\n", err)
	}
	if rowCnt != 1 {
		log.Fatalf("Update error: %d rows were updated instead of 1\n", rowCnt)
	}
}
