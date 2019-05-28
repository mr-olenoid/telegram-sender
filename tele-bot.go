package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	tgbotapi "github.com/Syfaro/telegram-bot-api"
	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
	ampq "github.com/streadway/amqp"
)

type subList struct {
	tag     string
	chatIds []int64
}

type message struct {
	Name     string `json:"name"`
	Message  string `json:"message"`
	Origin   string `json:"origin"`
	Severity string `json:"severity"`
	Format   string `json:"format"`
}

func failOnError(err error, msg string) {
	if err != nil {
		fmt.Printf("%s: %s", msg, err)

	}

}

func connectToDatabase() (db *sql.DB) {
	address := os.Getenv("DB_ADDRESS")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")
	connString := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s", user, password, address, dbName)
	db, err := sql.Open("mysql", connString) //use system variable as connection string
	failOnError(err, "Database connection error!")
	return db
}

func getSubs() (subs []subList, origins map[string]map[string]string) {
	var tags []string
	var tag string
	var chatID int64
	var sub subList

	dbCon := connectToDatabase()
	defer dbCon.Close()

	rows, err := dbCon.Query("SELECT name FROM tags")
	failOnError(err, "Selecting tags error!")
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&tag)
		tags = append(tags, tag)
	}
	for _, t := range tags {
		rows, err = dbCon.Query("SELECT chatID FROM subscribers WHERE tag = ?", t)
		failOnError(err, "Selecting subscribers error!")
		sub.tag = t
		for rows.Next() {
			rows.Scan(&chatID)
			sub.chatIds = append(sub.chatIds, chatID)
		}
		subs = append(subs, sub)
	}

	var selOrigin string
	var selOrigins []string
	var name string
	origins = make(map[string]map[string]string)

	rows, err = dbCon.Query("SELECT name from origins")
	failOnError(err, "Selecting origins error")
	for rows.Next() {
		rows.Scan(&selOrigin)
		selOrigins = append(selOrigins, selOrigin)
	}
	for _, originTmp := range selOrigins {
		origins[originTmp] = make(map[string]string)
		rows, err = dbCon.Query("SELECT tag, name from " + originTmp)
		failOnError(err, "Selecting servers error!")
		for rows.Next() {
			rows.Scan(&tag, &name)
			origins[originTmp][name] = tag
		}
	}
	return subs, origins
}

func queueWorker(subs []subList, origins map[string]map[string]string, msgs <-chan ampq.Delivery, bot *tgbotapi.BotAPI, wg *sync.WaitGroup) {
	defer wg.Done()
	var m message
	fmt.Println("RabbitMQ subprocess started")
	for d := range msgs {
		err := json.Unmarshal(d.Body, &m)
		if err != nil {
			log.Println("Unknown json data format!")
		} else {
			for _, sub := range subs {
				if sub.tag == origins[m.Origin][m.Name] {
					for _, chatID := range sub.chatIds {
						message := tgbotapi.NewMessage(chatID, m.Message)
						message.ParseMode = m.Format
						bot.Send(message)
					}
				}
			}
		}
	}
}

func rabbitConn(ip string, port string, user string, password string) (msgs <-chan ampq.Delivery, rabbitConnection *amqp.Connection, ch *ampq.Channel) {
	connectionString := fmt.Sprintf("amqp://%s:%s@%s:%s", user, password, ip, port)
	rabbitConnection, err := ampq.Dial(connectionString)
	failOnError(err, "Failed to connect to RabbitMQ")
	//defer rabbitConnection.Close()
	ch, err = rabbitConnection.Channel()
	failOnError(err, "Failed to open a channel")
	//defer ch.Close()
	err = ch.ExchangeDeclare(
		"alarms", // name
		"fanout", // type
		false,    // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")
	q, err := ch.QueueDeclare(
		"telegram", // name
		false,      // durable
		false,      // delete when usused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")
	err = ch.QueueBind(
		q.Name,
		"#",
		"alarms",
		false,
		nil)
	failOnError(err, "Failed to declare a queue")
	msgs, err = ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")
	return msgs, rabbitConnection, ch
}

func addRequest(chatID int64, firstName string, lastName string) string {
	dbConn := connectToDatabase()
	data, err := dbConn.Query("SELECT * FROM subscribers WHERE chatid = ?", chatID)
	failOnError(err, "Error checking subsribers!")
	defer data.Close()
	for data.Next() {
		return "Already subscribed!"
	}
	data, err = dbConn.Query("SELECT * FROM signUps WHERE chatid = ?", chatID)
	failOnError(err, "Error selecting signUps!")
	for data.Next() {
		return "Request pending!"
	}
	_, err = dbConn.Exec("INSERT INTO signUps (chatId, firstName, lastName) VALUES (?, ?, ?)", chatID, firstName, lastName)
	failOnError(err, "Error inserting request!")
	return "Request added to pool"
}

func telegramLogic(bot *tgbotapi.BotAPI, wg *sync.WaitGroup) {
	defer wg.Done()
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60 * 5
	updates, err := bot.GetUpdatesChan(u)
	failOnError(err, "Error on update")
	for update := range updates {
		if update.Message == nil { // ignore any non-Message Updates
			continue
		}
		if update.Message.Text == "/sign_up" {
			msg := tgbotapi.NewMessage(update.Message.Chat.ID, addRequest(update.Message.Chat.ID, update.Message.Chat.FirstName, update.Message.Chat.LastName))
			bot.Send(msg)
		}
	}
}

func main() {

	rabbitAddress := os.Getenv("RABBIT_ADDRESS")
	rabbitPort := os.Getenv("RABBIT_PORT")
	rabbitUser := os.Getenv("RABBIT_USER")
	rabbitPassword := os.Getenv("RABBIT_PASSWORD")

	botToken := os.Getenv("BOT_TOKEN")

	bot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil {
		log.Panic(err)
	}
	bot.Debug = true
	var wg sync.WaitGroup
	wg.Add(2)
	log.Printf("Authorized on account %s", bot.Self.UserName)
	msgs, conn, ch := rabbitConn(rabbitAddress, rabbitPort, rabbitUser, rabbitPassword)
	defer conn.Close()
	defer ch.Close()
	subs, origins := getSubs()
	go queueWorker(subs, origins, msgs, bot, &wg)
	go telegramLogic(bot, &wg)

	wg.Wait()
}
