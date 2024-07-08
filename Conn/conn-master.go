package connmaster

import (
	"crypto/aes"
	"crypto/cipher"

	// "database/sql"
	"encoding/base64"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func ConnMaster(DBUser, DBPass, DBName, DBHost, DBPort string) *sqlx.DB {
	// Ganti nilai-nilai berikut sesuai dengan informasi koneksi MySQL Anda
	dbUser := DBUser
	dbPass, err := decrypt(DBPass)
	if err != nil {
		log.Fatal(err)
	}
	// dbUsers := fmt.Sprintf("%s/", dbUser)
	index := strings.Index(dbPass, "/")
	dbPass = dbPass[index+1:]
	//dbPass = strings.Replace(dbPass, dbUsers, "/", 1)
	dbName := DBName
	dbHost := DBHost
	dbPort := DBPort

	// Format string untuk koneksi ke MySQL
	// username:pass@tcp(ip:port)/dbname
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPass, dbHost, dbPort, dbName)

	// Membuka koneksi ke database MySQL
	db, err := sqlx.Open("mysql", dataSourceName)
	if err != nil {
		log.Fatal(err)
	}

	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(1000)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(5 * time.Minute)

	return db

}

func ConnBilling(DBUser, DBPass, DBName, DBHost, DBPort string) *sqlx.DB {
	// Ganti nilai-nilai berikut sesuai dengan informasi koneksi MySQL Anda
	dbUser := DBUser
	dbPass := DBPass
	dbName := DBName
	dbHost := DBHost
	dbPort := DBPort

	// Format string untuk koneksi ke MySQL
	// username:pass@tcp(ip:port)/dbname
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPass, dbHost, dbPort, dbName)

	// Membuka koneksi ke database MySQL
	dbbilling, err := sqlx.Open("mysql", dataSourceName)
	if err != nil {
		log.Fatal(err)
	}

	// dbbilling.SetMaxIdleConns(100)
	// dbbilling.SetMaxOpenConns(1000)
	// dbbilling.SetConnMaxIdleTime(5 * time.Minute)
	// dbbilling.SetConnMaxLifetime(5 * time.Minute)

	return dbbilling

}

func ConnLoket(DBUser, DBPass, DBName, DBHost, DBPort string) *sqlx.DB {
	// Ganti nilai-nilai berikut sesuai dengan informasi koneksi MySQL Anda
	dbUser := DBUser
	dbPass := DBPass
	dbName := DBName
	dbHost := DBHost
	dbPort := DBPort

	// Format string untuk koneksi ke MySQL
	// username:pass@tcp(ip:port)/dbname
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPass, dbHost, dbPort, dbName)

	// Membuka koneksi ke database MySQL
	dbloket, err := sqlx.Open("mysql", dataSourceName)
	if err != nil {
		log.Fatal(err)
	}

	// dbloket.SetMaxIdleConns(100)
	// dbloket.SetMaxOpenConns(1000)
	// dbloket.SetConnMaxIdleTime(5 * time.Minute)
	// dbloket.SetConnMaxLifetime(5 * time.Minute)

	return dbloket

}

func ConnAkun(DBUser, DBPass, DBName, DBHost, DBPort string) *sqlx.DB {
	// Ganti nilai-nilai berikut sesuai dengan informasi koneksi MySQL Anda
	dbUser := DBUser
	dbPass := DBPass
	dbName := DBName
	dbHost := DBHost
	dbPort := DBPort

	// Format string untuk koneksi ke MySQL
	// username:pass@tcp(ip:port)/dbname
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPass, dbHost, dbPort, dbName)

	// Membuka koneksi ke database MySQL
	dbakun, err := sqlx.Open("mysql", dataSourceName)
	if err != nil {
		log.Fatal(err)
	}

	// dbakun.SetMaxIdleConns(10)
	// dbakun.SetMaxOpenConns(100)
	// dbakun.SetConnMaxIdleTime(5 * time.Minute)
	// dbakun.SetConnMaxLifetime(5 * time.Minute)

	return dbakun

}

func ConnWr(DBUser, DBPass, DBName, DBHost, DBPort string) *sqlx.DB {
	// Ganti nilai-nilai berikut sesuai dengan informasi koneksi MySQL Anda
	dbUser := DBUser
	dbPass := DBPass
	dbName := DBName
	dbHost := DBHost
	dbPort := DBPort

	// Format string untuk koneksi ke MySQL
	// username:pass@tcp(ip:port)/dbname
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPass, dbHost, dbPort, dbName)

	// Membuka koneksi ke database MySQL
	dbwr, err := sqlx.Open("mysql", dataSourceName)
	if err != nil {
		log.Fatal(err)
	}

	// dbwr.SetMaxIdleConns(10)
	// dbwr.SetMaxOpenConns(100)
	// dbwr.SetConnMaxIdleTime(5 * time.Minute)
	// dbwr.SetConnMaxLifetime(5 * time.Minute)

	return dbwr

}

func decrypt(cryptoText string) (string, error) {
	key := []byte("inikeybsa2023encryptionkeamanans") // Harus 32 bytes untuk AES-256

	ciphertext, err := base64.StdEncoding.DecodeString(cryptoText)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	if len(ciphertext) < aes.BlockSize {
		return "", fmt.Errorf("teks terenkripsi terlalu pendek")
	}

	iv := ciphertext[:aes.BlockSize]
	ciphertext = ciphertext[aes.BlockSize:]

	mode := cipher.NewCBCDecrypter(block, iv)
	plaintext := make([]byte, len(ciphertext))
	mode.CryptBlocks(plaintext, ciphertext)

	// Menghapus padding dari teks yang terdekripsi
	plaintext = PKCS5Unpadding(plaintext)

	return string(plaintext), nil
}

func PKCS5Unpadding(plaintext []byte) []byte {
	length := len(plaintext)
	unpadding := int(plaintext[length-1])
	return plaintext[:(length - unpadding)]
}
