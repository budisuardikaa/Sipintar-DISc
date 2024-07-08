package main

import (
	connmaster "SiPriksa/Conn"
	procc "SiPriksa/Process"
	telebot "SiPriksa/Telegram-Bot"
	"encoding/json"
	"os"

	"context"
	_ "embed"
	"fmt"

	//"io/ioutil"
	"log"

	"github.com/joho/godotenv"
)

type Config struct {
	DBUser string `json:"DBUser"`
	DBPass string `json:"DBPass"`
	DBName string `json:"DBName"`
	DBHost string `json:"DBHost"`
	DBPort string `json:"DBPort"`
	IdPDAM string `json:"IdPDAM"`
}

type DataConfig struct {
	DBUser string `json:"DBUser"`
	DBPass string `json:"DBPass"`
	DBName string `json:"DBName"`
	DBHost string `json:"DBHost"`
	DBPort string `json:"DBPort"`
	IdPDAM string `json:"IdPDAM"`
}

type Data struct {
	Tipe   string `json:"tipe"`
	DBHost string `json:"dbhost"`
	DBUser string `json:"username"`
	DBPass string `json:"password"`
	DBName string `json:"DBName"`
	DBPort string `json:"port"`
}

//go:embed config.json
var envContent string

func main() {
	var pesan, header string
	//header = "======================================= \n \nCopyright (C) SiPriksa - 1.0.0. All right reserved. \n \n======================================="
	// lines := strings.Split(envContent, "\n")

	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	// DBHost := os.Getenv("DBHost")
	// DBPort := os.Getenv("DBPort")
	// DBName := os.Getenv("DBName")
	// DBUser := os.Getenv("DBUser")
	// DBPass := os.Getenv("DBPass")
	IdPDAM := os.Getenv("ID_PDAM")

	// var DBHost, DBPort, DBName, DBUser, DBPass, IdPDAM string
	// for _, line := range lines {
	// 	parts := strings.SplitN(line, "=", 2)
	// 	if len(parts) == 2 {
	// 		switch parts[0] {
	// 		case "DB_HOST":
	// 			DBHost = parts[1]
	// 		case "DB_PORT":
	// 			DBPort = parts[1]
	// 		case "DB_NAME":
	// 			DBName = parts[1]
	// 		case "DB_USER":
	// 			DBUser = parts[1]
	// 		case "DB_PASSWORD":
	// 			DBPass = parts[1]
	// 		case "ID_PDAM":
	// 			IdPDAM = parts[1]
	// 		}
	// 	}
	// }
	// fmt.Printf("DB Host: %s\n", DBHost)
	// fmt.Printf("DB Port: %s\n", DBPort)
	// fmt.Printf("DB Name: %s\n", DBName)
	// fmt.Printf("DB User: %s\n", DBUser)
	// fmt.Printf("DB Pass: %s\n", DBPass)
	fmt.Printf("ID PDAM: %s\n", IdPDAM)

	config, err := readJSONConf(envContent)
	if err != nil {
		log.Fatal("Gagal membaca konfigurasi:", err)
	}
	// var DBHost, DBPort, DBName, DBUser, DBPass, IdPDAM string
	var DBHost, DBPort, DBName, DBUser, DBPass string

	for _, data := range config {
		DBHost = data.DBHost
		DBPort = data.DBPort
		DBName = data.DBName
		DBUser = data.DBUser
		DBPass = data.DBPass
		// IdPDAM = data.IdPDAM
	}

	//START PING BERULANG
	//for i := 1; i <= 10; i++ {
	// for {
	// 	db := connmaster.ConnMaster(DBUser, DBPass, DBName, DBHost, DBPort)
	// 	defer db.Close()

	// 	// Mengecek koneksi ke database
	// 	err := db.Ping()
	// 	if err != nil {
	// 		fmt.Println("Tidak dapat terhubung ke database:", err)
	// 		goto Sleep
	// 	} else {

	// 		break

	// 	}
	// Sleep:
	// 	time.Sleep(5 * time.Second)
	// }
	// END PING BERULANG

	// CEK PDAM
	//db := connmaster.ConnMaster(DBUser, DBPass, DBName, DBHost, DBPort)
	db := connmaster.ConnMaster(DBUser, DBPass, DBName, DBHost, DBPort)
	defer db.Close()
	ctx := context.Background()
	//var db *sql.DB
	script := "SELECT idpdam,nama_pdam FROM sipintar_pdam_config WHERE idpdam = ? "
	// rows, err := db.QueryContext(ctx, script, IdPDAM)
	rows, err := db.QueryContext(ctx, script, IdPDAM)
	if err != nil {
		log.Fatalf("Gagal ambil data : %v", err)
		//goto Sleep2
	}

	var idpdam, nama_pdam string
	for rows.Next() {
		err := rows.Scan(&idpdam, &nama_pdam)
		if err != nil {
			log.Fatalf("Data kosong : %v", err)
		}
	}
	header = "=======================================\n \nNama PDAM : " + nama_pdam + "\n \n======================================= \n"

	// END CEK PDAM
	fmt.Println(header)
	//START CEK VERSION
	fmt.Println("\n- CEK VERSION")
	pesan = procc.CekVersionBill(DBUser, DBPass, DBName, DBHost, DBPort, IdPDAM, pesan)
	pesan = procc.CekVersionLoket(DBUser, DBPass, DBName, DBHost, DBPort, IdPDAM, pesan)
	pesan = procc.CekVersionBshpd(DBUser, DBPass, DBName, DBHost, DBPort, IdPDAM, pesan)
	// END CEK VERSION
	pesan = pesan + "\n \n======================================= \n"

	// START CEK SELISIH PELANGGAN
	pesan = procc.CekSelisihPelanggan(DBUser, DBPass, DBName, DBHost, DBPort, IdPDAM, pesan)
	// END CEK SELISIH PELANGGAN
	pesan = pesan + "\n======================================= \n"

	//START CEK STATUS PELANGGAN LOKET - BILLING
	pesan = procc.CekPerubahanPelanggan(DBUser, DBPass, DBName, DBHost, DBPort, IdPDAM, pesan)
	// END CEK STATUS PELANGGAN LOKET - BILLING
	pesan = pesan + "\n======================================= \n"

	// //START CEK LPP LOKET - BILLING
	pesan = procc.CekLPP(DBUser, DBPass, DBName, DBHost, DBPort, IdPDAM, pesan)
	// // END CEK LPP LOKET - BILLING
	pesan = pesan + "\n======================================= \n"

	//START CEK PEMBATALAN LOKET - BILLING
	pesan = procc.CekPembatalan(DBUser, DBPass, DBName, DBHost, DBPort, IdPDAM, pesan)
	// END CEK PEMBATALAN LOKET - BILLING
	pesan = pesan + "\n======================================= \n"

	//START CEK SELISIH PIUTANG
	pesan = procc.CekPiutang(DBUser, DBPass, DBName, DBHost, DBPort, IdPDAM, pesan)
	// END CEK SELISIH PIUTANG
	pesan = pesan + "\n======================================= \n"

	result := header + pesan
	start := "=======================================\n \n WORK START FROM HERE \n \n======================================= \n"
	// result = ""
	// start = ""
	fmt.Println(start)
	// fmt.Println(result)

	if result != "" {
		telebot.TeleBot(result, nama_pdam)
	}

}

// func readJSONArray(jsonString string) ([]Data, error) {
// 	var dataList []Data

// 	// Mengonversi string JSON ke dalam slice dari struktur data
// 	err := json.Unmarshal([]byte(jsonString), &dataList)
// 	if err != nil {
// 		return dataList, err
// 	}

// 	return dataList, nil
// }

func readJSONConf(jsonString string) ([]DataConfig, error) {
	var dataConf []DataConfig

	// Mengonversi string JSON ke dalam slice dari struktur data
	err := json.Unmarshal([]byte(jsonString), &dataConf)
	if err != nil {
		return dataConf, err
	}

	return dataConf, nil
}
