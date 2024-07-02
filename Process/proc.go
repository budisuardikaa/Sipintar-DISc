package procc

import (
	connmaster "SiPriksa/Conn"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Data struct {
	Tipe   string `json:"tipe"`
	DBHost string `json:"dbhost"`
	DBUser string `json:"username"`
	DBPass string `json:"password"`
	DBName string `json:"DBName"`
	DBPort string `json:"port"`
}

// type Ver struct {
// 	kode string
// 	ver  string
// }

type NosambBill struct {
	Nosamb string
	// tambahkan fields lain sesuai kebutuhan
}

type NosambLoket struct {
	Nosamb string
	Nama   string
}

type DataPelLoket struct {
	Id               int
	Nosamb           string
	Nama             string
	Alamat           string
	Nohp             string
	Kodekolektif     string
	Status           string
	Flag             string
	Kodekondisimeter string
	kodegol          string
	Kodeblok         string
	Koderayon        string
	Kodekelurahan    string
	Keterangan       string
}

type DataPelBill struct {
	Id               int
	Nosamb           string
	Nama             string
	Alamat           string
	Nohp             string
	Kodekolektif     string
	Status           string
	Flag             string
	Kodekondisimeter string
	kodegol          string
	Kodeblok         string
	Koderayon        string
	Kodekelurahan    string
}

type TglLppLoket struct {
	Id         int
	TglBayar   string
	Jumlah     int
	Rekair     float64
	Keterangan string
}

type TglLppBill struct {
	TglBayar string
	Jumlah   int
	Rekair   float64
}

type DataLppLoket struct {
	Id         int
	Kode       string
	TglBayar   string
	Rekair     float64
	Keterangan string
}

type DataLppBill struct {
	Id       int
	Kode     string
	TglBayar string
	Rekair   float64
}

type TglAngsLoket struct {
	Id         int
	TglBayar   string
	Jumlah     int
	Rekair     float64
	Keterangan string
}

type TglAngsBill struct {
	TglBayar string
	Jumlah   int
	Rekair   float64
}

type DataAngsLoket struct {
	Id         int
	Kode       string
	Jenis      string
	TglBayar   string
	Rekair     float64
	Keterangan string
}

type DataAngsBill struct {
	Kode     string
	Jenis    string
	TglBayar string
	Rekair   float64
}

type TglAngsNonL struct {
	Id         int
	TglBayar   string
	Jumlah     int
	Total      float64
	Keterangan string
}

type TglAngsNonB struct {
	TglBayar string
	Jumlah   int
	Total    float64
}

type DataAngsNonL struct {
	Id         int
	Kode       string
	Jenis      string
	TglBayar   string
	Total      float64
	Keterangan string
}

type DataAngsNonB struct {
	Kode     string
	Jenis    string
	TglBayar string
	Total    float64
}

type TglNonL struct {
	Id         int
	TglBayar   string
	Jumlah     int
	Total      float64
	Keterangan string
}

type TglNonB struct {
	TglBayar string
	Jumlah   int
	Total    float64
}

type DataNonL struct {
	Id         int
	Kode       string
	Jenis      string
	TglBayar   string
	Total      float64
	Keterangan string
}

type DataNonB struct {
	Kode     string
	Jenis    string
	TglBayar string
	Total    float64
}

type KodePAirL struct {
	Id         int
	Kode       string
	Rekair     float64
	Keterangan string
}

type KodePAirB struct {
	Kode   string
	Rekair float64
}

type KodePAngAirL struct {
	Id         int
	Kode       string
	Rekair     float64
	Keterangan string
}

type KodePAngAirB struct {
	Kode   string
	Rekair float64
}

type KodePAngNonAirL struct {
	Id         int
	Kode       string
	Total      float64
	Keterangan string
}

type KodePAngNonAirB struct {
	Kode  string
	Total float64
}

type KodePNonAirL struct {
	Id         int
	Kode       string
	Total      float64
	Keterangan string
}

type KodePNonAirB struct {
	Kode  string
	Total float64
}

type Periode struct {
	Kode    string
	bulan   string
	Tgllalu string
	Tglkini string
	Kode2   string
}

type Periods struct {
	Tahun string
	Kode  string
	Kode2 string
}

type Piutang struct {
	Nosamb        string
	Awal_Piutang  float64
	Awal_Bayar    float64
	Lpp           float64
	Posting       float64
	Akhir_Piutang float64
	Akhir_Bayar   float64
}

type Koreksi struct {
	Nosamb string
	Awal1  float64
	Awal2  float64
	Akhir  float64
}

func (p *Piutang) Reset1() {
	p.Nosamb = ""
	p.Awal_Piutang = 0
	p.Awal_Bayar = 0
	p.Lpp = 0
	p.Posting = 0
	p.Akhir_Piutang = 0
	p.Akhir_Bayar = 0
}

func (k *Koreksi) Reset3() {
	k.Nosamb = ""
	k.Awal1 = 0
	k.Awal2 = 0
	k.Akhir = 0
}

func CekVersionBill(DBUser, DBPass, DBName, DBHost, DBPort, IdPDAM, pesan string) string {
	// Ganti nilai-nilai berikut sesuai dengan informasi koneksi MySQL Anda
	dbUser := DBUser
	dbPass := DBPass
	dbName := DBName
	dbHost := DBHost
	dbPort := DBPort
	idPDAM := IdPDAM

	for {
		db := connmaster.ConnMaster(dbUser, dbPass, dbName, dbHost, dbPort)
		defer db.Close()

		// Mengecek koneksi ke database
		err := db.Ping()
		if err != nil {
			fmt.Println("Tidak dapat terhubung ke database: ", err)
			// pesan = "Tidak dapat terhubung ke database:" + err.Error()

			goto Sleep
		} else {

			break

		}
	Sleep:
		time.Sleep(5 * time.Second)
	}

	db := connmaster.ConnMaster(dbUser, dbPass, dbName, dbHost, dbPort)
	defer db.Close()

	//START AMBIL VERSION RELEASE
	cek_version := context.Background()
	s_cekversion := "SELECT nama_modul,version_release FROM sipintar_modul_aplikasi where version_release<>'-' AND nama_modul='BILLING' "
	r_cekversion, e_cekversion := db.QueryContext(cek_version, s_cekversion)
	if e_cekversion != nil {
		log.Fatal("Gagal ambil data : ", e_cekversion)
	}

	for r_cekversion.Next() {
		var nama_module, version_release string
		err := r_cekversion.Scan(&nama_module, &version_release)
		if err != nil {
			log.Fatal("Data kosong : ", err)
		}

		//var bill_ver, loket_ver, bshpd_ver string
		// if nama_module == "BILLING" {
		// 	bill_ver := Ver{nama_module, version_release}
		// 	fmt.Println("Version Release Billing:", bill_ver.ver)
		// }

		idpdam := idPDAM
		ctx := context.Background()
		//var db *sql.DB
		script := "SELECT idpdam,nama_pdam,`database` FROM sipintar_pdam_config WHERE idpdam = ? "
		rows, err := db.QueryContext(ctx, script, idpdam)
		if err != nil {
			log.Fatal("Gagal ambil data : ", err)
			//goto Sleep2
		}

		for rows.Next() {
			var idpdam, nama_pdam, database string
			err := rows.Scan(&idpdam, &nama_pdam, &database)
			if err != nil {
				log.Fatal("Data kosong : ", err)
			}

			//Menampung Array hasil select config database
			databs, err := readJSONArray(database)
			if err != nil {
				log.Fatal("Gagal membaca data :", err)
			}

			var Ip_billing, User_billing, Pass_billing, DB_billing, Port_billing string

			for _, data := range databs {

				if data.Tipe == "billing" {
					Ip_billing = data.DBHost
					User_billing = data.DBUser
					Pass_billing = data.DBPass
					DB_billing = data.DBName
					Port_billing = data.DBPort

				}

			}

			//KONEK KE DB BILLING
			dbbilling := connmaster.ConnBilling(User_billing, Pass_billing, DB_billing, Ip_billing, Port_billing)
			defer dbbilling.Close()

			// Mengecek koneksi ke database Billing
			err = dbbilling.Ping()
			if err != nil {
				log.Fatal("Tidak dapat terhubung ke database: ", err)
			}

			ctx_vbill := context.Background()

			cekversionbill, e_cekversion := dbbilling.QueryContext(ctx_vbill, "SELECT `VERSION` FROM pengaturan ")
			if e_cekversion != nil {
				log.Fatal("Gagal ambil data : ", err)
			}
			for cekversionbill.Next() {
				var version string
				err := cekversionbill.Scan(&version)
				if err != nil {
					log.Fatal("Data kosong :", err)
				}

				// fmt.Println("Version billing terpasang:", version)
				pesan = pesan + "\nCEK VERSION BILLING \n"
				if version_release != version {
					pesan = pesan + "\n" + "Versi Billing terpasang : \n" + version
					pesan = pesan + "\n" + "Versi Billing release terbaru : \n" + version_release

					fmt.Println("Versi Billing berbeda! Terpasang : ", version, " , Release : ", version_release)

					pesan = pesan + "\n \nVersi billing yang terpasang lama, mohon segera lakukan update!"
				} else {
					fmt.Println("Versi Billing sudah sama!")

					pesan = pesan + "\nVersi billing yang terpasang sudah paling terbaru!"
				}

			}

			defer cekversionbill.Close()
		}

		defer rows.Close()
	}

	return pesan
}

func CekVersionLoket(DBUser, DBPass, DBName, DBHost, DBPort, IdPDAM, pesan string) string {
	// Ganti nilai-nilai berikut sesuai dengan informasi koneksi MySQL Anda
	dbUser := DBUser
	dbPass := DBPass
	dbName := DBName
	dbHost := DBHost
	dbPort := DBPort
	idPDAM := IdPDAM

	for {
		db := connmaster.ConnMaster(dbUser, dbPass, dbName, dbHost, dbPort)
		defer db.Close()

		// Mengecek koneksi ke database
		err := db.Ping()
		if err != nil {
			fmt.Println("Tidak dapat terhubung ke database:", err)
			// pesan = "Tidak dapat terhubung ke database:" + err.Error()
			// telebot.TeleBot(pesan)
			goto Sleep
		} else {

			break

		}
	Sleep:
		time.Sleep(5 * time.Second)
	}

	db := connmaster.ConnMaster(dbUser, dbPass, dbName, dbHost, dbPort)
	defer db.Close()

	//START AMBIL VERSION RELEASE
	cek_version := context.Background()
	s_cekversion := "SELECT nama_modul,version_release FROM sipintar_modul_aplikasi where version_release<>'-' AND nama_modul='LOKET' "
	r_cekversion, e_cekversion := db.QueryContext(cek_version, s_cekversion)
	if e_cekversion != nil {
		log.Fatal("Gagal ambil data : ", e_cekversion)
	}

	for r_cekversion.Next() {
		var nama_module, version_release string
		err := r_cekversion.Scan(&nama_module, &version_release)
		if err != nil {
			log.Fatal("Data kosong : ", err)
		}

		//var bill_ver, loket_ver, bshpd_ver string
		// if nama_module == "LOKET" {
		// 	loket_ver := Ver{nama_module, version_release}
		// 	fmt.Println("Version Release Loket:", loket_ver.ver)
		// }

		idpdam := idPDAM
		ctx := context.Background()
		//var db *sql.DB
		script := "SELECT idpdam,nama_pdam,`database` FROM sipintar_pdam_config WHERE idpdam = ? "
		rows, err := db.QueryContext(ctx, script, idpdam)
		if err != nil {
			log.Fatal("Gagal ambil data : ", err)
			//goto Sleep2
		}

		for rows.Next() {
			var idpdam, nama_pdam, database string
			err := rows.Scan(&idpdam, &nama_pdam, &database)
			if err != nil {
				log.Fatal("Data kosong : ", err)
			}

			//Menampung Array hasil select config database
			databs, err := readJSONArray(database)
			if err != nil {
				log.Fatal("Gagal membaca data :", err)
			}

			var Ip_loket, User_loket, Pass_loket, DB_loket, Port_loket string

			for _, data := range databs {

				if data.Tipe == "loket" {
					Ip_loket = data.DBHost
					User_loket = data.DBUser
					Pass_loket = data.DBPass
					DB_loket = data.DBName
					Port_loket = data.DBPort

				}

			}

			//KONEK KE DB BILLING
			dbloket := connmaster.ConnLoket(User_loket, Pass_loket, DB_loket, Ip_loket, Port_loket)
			defer dbloket.Close()

			// Mengecek koneksi ke database Billing
			err = dbloket.Ping()
			if err != nil {
				log.Fatal("Tidak dapat terhubung ke database: ", err)
			}

			ctx_vloket := context.Background()

			cekversionlok, e_cekversion := dbloket.QueryContext(ctx_vloket, "SELECT `version` FROM pengaturan ")
			if e_cekversion != nil {
				log.Fatal("Gagal ambil data : ", err)
			}

			for cekversionlok.Next() {
				var version string
				err := cekversionlok.Scan(&version)
				if err != nil {
					log.Fatal("Data kosong :", err)
				}

				// fmt.Println("Version billing terpasang:", version)
				pesan = pesan + "\n \nCEK VERSION LOKET \n"
				if version_release != version {
					pesan = pesan + "\n" + "Versi Loket terpasang : \n" + version
					pesan = pesan + "\n" + "Versi Loket release terbaru : \n" + version_release
					fmt.Println("Versi Loket berbeda! Terpasang : ", version, " , Release : ", version_release)
					pesan = pesan + "\n \nVersi Loket yang terpasang lama, mohon segera lakukan update!"
				} else {
					fmt.Println("Versi Loket sudah sama!")
					pesan = pesan + "\nVersi Loket yang terpasang sudah paling terbaru!"
				}

			}

			defer cekversionlok.Close()
		}

		defer rows.Close()
	}

	return pesan
}

func CekVersionBshpd(DBUser, DBPass, DBName, DBHost, DBPort, IdPDAM, pesan string) string {
	// Ganti nilai-nilai berikut sesuai dengan informasi koneksi MySQL Anda
	dbUser := DBUser
	dbPass := DBPass
	dbName := DBName
	dbHost := DBHost
	dbPort := DBPort
	idPDAM := IdPDAM

	for {
		db := connmaster.ConnMaster(dbUser, dbPass, dbName, dbHost, dbPort)
		defer db.Close()

		// Mengecek koneksi ke database
		err := db.Ping()
		if err != nil {
			fmt.Println("Tidak dapat terhubung ke database:", err)
			// pesan = "Tidak dapat terhubung ke database:" + err.Error()
			// telebot.TeleBot(pesan)
			goto Sleep
		} else {

			break

		}
	Sleep:
		time.Sleep(5 * time.Second)
	}

	db := connmaster.ConnMaster(dbUser, dbPass, dbName, dbHost, dbPort)
	defer db.Close()

	//START AMBIL VERSION RELEASE
	cek_version := context.Background()
	s_cekversion := "SELECT nama_modul,version_release FROM sipintar_modul_aplikasi where version_release<>'-' AND nama_modul='BSHPD' "
	r_cekversion, e_cekversion := db.QueryContext(cek_version, s_cekversion)
	if e_cekversion != nil {
		log.Println("Gagal ambil data : ", e_cekversion)
	}

	for r_cekversion.Next() {
		var nama_module, version_release string
		err := r_cekversion.Scan(&nama_module, &version_release)
		if err != nil {
			log.Fatal("Data kosong : ", err)
		}

		//var bill_ver, loket_ver, bshpd_ver string
		// if nama_module == "BSHPD" {
		// 	loket_ver := Ver{nama_module, version_release}
		// 	fmt.Println("Version Release BSHPD:", loket_ver.ver)
		// }

		idpdam := idPDAM
		ctx := context.Background()
		//var db *sql.DB
		script := "SELECT idpdam,nama_pdam,`database` FROM sipintar_pdam_config WHERE idpdam = ? "
		rows, err := db.QueryContext(ctx, script, idpdam)
		if err != nil {
			log.Fatal("Gagal ambil data : ", err)
			//goto Sleep2
		}

		for rows.Next() {
			var idpdam, nama_pdam, database string
			err := rows.Scan(&idpdam, &nama_pdam, &database)
			if err != nil {
				log.Fatal("Data kosong : ", err)
			}

			//Menampung Array hasil select config database
			databs, err := readJSONArray(database)
			if err != nil {
				log.Fatal("Gagal membaca data :", err)
			}

			var Ip_loket, User_loket, Pass_loket, DB_loket, Port_loket string

			for _, data := range databs {

				if data.Tipe == "loket" {
					Ip_loket = data.DBHost
					User_loket = data.DBUser
					Pass_loket = data.DBPass
					DB_loket = data.DBName
					Port_loket = data.DBPort

				}

			}

			//KONEK KE DB BILLING
			dbloket := connmaster.ConnLoket(User_loket, Pass_loket, DB_loket, Ip_loket, Port_loket)
			defer dbloket.Close()

			// Mengecek koneksi ke database Billing
			err = dbloket.Ping()
			if err != nil {
				log.Fatal("Tidak dapat terhubung ke database: ", err)
			}

			ctx_vloket := context.Background()

			cekversionlok, e_cekversion := dbloket.QueryContext(ctx_vloket, "SELECT `versi` FROM updateexe_bshpd ")
			if e_cekversion != nil {
				log.Fatal("Gagal ambil data : ", err)
			}

			for cekversionlok.Next() {
				var versi string
				err := cekversionlok.Scan(&versi)
				if err != nil {
					log.Fatal("Data kosong : ", err)
				}

				// fmt.Println("Version billing terpasang:", version)
				pesan = pesan + "\n \nCEK VERSION BSHPD \n"
				if version_release != versi {
					pesan = pesan + "\n" + "Versi BHSPD terpasang : \n" + versi
					pesan = pesan + "\n" + "Versi BHSPD release terbaru : \n" + version_release
					fmt.Println("Versi BSHPD berbeda! Terpasang : ", versi, " , Release : ", version_release)
					pesan = pesan + "\n \nVersi BHSPD yang terpasang lama, mohon segera lakukan update!"
				} else {
					fmt.Println("Versi BSHPD sudah sama!")
					pesan = pesan + "\nVersi BHSPD yang terpasang sudah paling terbaru!"
				}

			}

			defer cekversionlok.Close()
		}

		defer rows.Close()
	}

	return pesan
}

func CekSelisihPelanggan(DBUser, DBPass, DBName, DBHost, DBPort, IdPDAM, pesan string) string {
	// Ganti nilai-nilai berikut sesuai dengan informasi koneksi MySQL Anda
	dbUser := DBUser
	dbPass := DBPass
	dbName := DBName
	dbHost := DBHost
	dbPort := DBPort
	idPDAM := IdPDAM

	fmt.Println("\n- CEK SELISIH PELANGGAN")

	for {
		db := connmaster.ConnMaster(dbUser, dbPass, dbName, dbHost, dbPort)
		defer db.Close()

		// Mengecek koneksi ke database
		err := db.Ping()
		if err != nil {
			fmt.Println("Tidak dapat terhubung ke database:", err)
			// pesan = "Tidak dapat terhubung ke database:" + err.Error()
			// telebot.TeleBot(pesan)
			goto Sleep
		} else {

			break

		}
	Sleep:
		time.Sleep(5 * time.Second)
	}

	// START CEK PELANGGAN BILLING
	db := connmaster.ConnMaster(dbUser, dbPass, dbName, dbHost, dbPort)
	defer db.Close()

	ctx := context.Background()
	//var db *sql.DB
	script := "SELECT idpdam,nama_pdam,`database` FROM sipintar_pdam_config WHERE idpdam = ? "
	rows, err := db.QueryContext(ctx, script, idPDAM)
	if err != nil {
		log.Fatal("Gagal ambil data : ", err)
		//goto Sleep2
	}

	for rows.Next() {
		var idpdam, nama_pdam, database string
		err := rows.Scan(&idpdam, &nama_pdam, &database)
		if err != nil {
			log.Fatal("Data kosong : ", err)
		}

		//Menampung Array hasil select config database
		databs, err := readJSONArray(database)
		if err != nil {
			log.Fatal("Gagal membaca data :", err)
		}

		var Ip_bill, User_bill, Pass_bill, DB_bill, Port_bill string
		var Ip_loket, User_loket, Pass_loket, DB_loket, Port_loket string

		for _, data := range databs {

			if data.Tipe == "billing" {
				Ip_bill = data.DBHost
				User_bill = data.DBUser
				Pass_bill = data.DBPass
				DB_bill = data.DBName
				Port_bill = data.DBPort

			}
			if data.Tipe == "loket" {
				Ip_loket = data.DBHost
				User_loket = data.DBUser
				Pass_loket = data.DBPass
				DB_loket = data.DBName
				Port_loket = data.DBPort

			}

		}

		for {
			dbbilling := connmaster.ConnBilling(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)
			defer dbbilling.Close()

			// Mengecek koneksi ke database
			err := dbbilling.Ping()
			if err != nil {
				fmt.Println("Tidak dapat terhubung ke database:", err)
				// pesan = "Tidak dapat terhubung ke database:" + err.Error()
				// telebot.TeleBot(pesan)
				goto SleepB1
			} else {

				break

			}
		SleepB1:
			time.Sleep(5 * time.Second)
		}

		//KONEK KE DB BILLING
		dbbill := connmaster.ConnBilling(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)
		defer dbbill.Close()

		// Mengecek koneksi ke database Billing
		err = dbbill.Ping()
		if err != nil {
			log.Fatal("Tidak dapat terhubung ke database: ", err)
		}

		fmt.Print("AMBIL PELANGGAN BILLING")
		nosambBill, err := getPelangganBill(dbbill)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(" [DONE]")

		for {
			dbloket := connmaster.ConnLoket(User_loket, Pass_loket, DB_loket, Ip_loket, Port_loket)
			defer dbloket.Close()

			// Mengecek koneksi ke database
			err := dbloket.Ping()
			if err != nil {
				fmt.Println("Tidak dapat terhubung ke database:", err)
				// pesan = "Tidak dapat terhubung ke database:" + err.Error()
				// telebot.TeleBot(pesan)
				goto SleepL1
			} else {

				break

			}
		SleepL1:
			time.Sleep(5 * time.Second)
		}

		//KONEK KE DB LOKET
		dbloket := connmaster.ConnLoket(User_loket, Pass_loket, DB_loket, Ip_loket, Port_loket)
		defer dbloket.Close()

		// Mengecek koneksi ke database Loket
		err = dbloket.Ping()
		if err != nil {
			log.Fatal("Tidak dapat terhubung ke database: ", err)
		}
		fmt.Print("CEK PELANGGAN BILLING BELUM MASUK")
		pesan = pesan + "\nCEK PELANGGAN LOKET BELUM MASUK BILLING \n \n"
		pesan, err = getPelangganLoket(dbloket, nosambBill, pesan)
		if err != nil {
			log.Fatal(err)
		}

	}

	defer rows.Close()

	return pesan
}

func CekPerubahanPelanggan(DBUser, DBPass, DBName, DBHost, DBPort, IdPDAM, pesan string) string {
	// Ganti nilai-nilai berikut sesuai dengan informasi koneksi MySQL Anda
	dbUser := DBUser
	dbPass := DBPass
	dbName := DBName
	dbHost := DBHost
	dbPort := DBPort
	idPDAM := IdPDAM
	fmt.Println("\n- CEK PERUBAHAN PELANGGAN")

	for {
		db := connmaster.ConnMaster(dbUser, dbPass, dbName, dbHost, dbPort)
		defer db.Close()

		// Mengecek koneksi ke database
		err := db.Ping()
		if err != nil {
			log.Fatal("Tidak dapat terhubung ke database:", err)
			goto Sleep
		} else {

			break

		}
	Sleep:
		time.Sleep(5 * time.Second)
	}

	// START CEK PELANGGAN BILLING
	db := connmaster.ConnMaster(dbUser, dbPass, dbName, dbHost, dbPort)
	defer db.Close()

	ctx := context.Background()
	//var db *sql.DB
	script := "SELECT idpdam,nama_pdam,`database` FROM sipintar_pdam_config WHERE idpdam = ? "
	rows, err := db.QueryContext(ctx, script, idPDAM)
	if err != nil {
		log.Fatal("Gagal ambil data : ", err)
	}

	var dataPelLoket []DataPelLoket

	for rows.Next() {
		var idpdam, nama_pdam, database string
		err := rows.Scan(&idpdam, &nama_pdam, &database)
		if err != nil {
			log.Fatal("Data kosong : ", err)
		}

		//Menampung Array hasil select config database
		databs, err := readJSONArray(database)
		if err != nil {
			log.Fatal("Gagal membaca data :", err)
		}

		var Ip_bill, User_bill, Pass_bill, DB_bill, Port_bill string
		var Ip_loket, User_loket, Pass_loket, DB_loket, Port_loket string

		for _, data := range databs {

			if data.Tipe == "billing" {
				Ip_bill = data.DBHost
				User_bill = data.DBUser
				Pass_bill = data.DBPass
				DB_bill = data.DBName
				Port_bill = data.DBPort

			}
			if data.Tipe == "loket" {
				Ip_loket = data.DBHost
				User_loket = data.DBUser
				Pass_loket = data.DBPass
				DB_loket = data.DBName
				Port_loket = data.DBPort

			}

		}

		for {
			dbloket := connmaster.ConnLoket(User_loket, Pass_loket, DB_loket, Ip_loket, Port_loket)
			defer dbloket.Close()

			// Mengecek koneksi ke database
			err := dbloket.Ping()
			if err != nil {
				fmt.Println("Tidak dapat terhubung ke database:", err)
				// pesan = "Tidak dapat terhubung ke database:" + err.Error()
				// telebot.TeleBot(pesan)
				goto SleepL1
			} else {

				break

			}
		SleepL1:
			time.Sleep(5 * time.Second)
		}

		//KONEK KE DB LOKET
		dbloket := connmaster.ConnLoket(User_loket, Pass_loket, DB_loket, Ip_loket, Port_loket)
		defer dbloket.Close()

		// Mengecek koneksi ke database Loket
		err = dbloket.Ping()
		if err != nil {
			log.Fatal("Tidak dapat terhubung ke database: ", err)
		}

		fmt.Print("AMBIL PELANGGAN LOKET")
		ctx_pelloket := context.Background()
		tahun_update := time.Now().Year() - 1
		q_pelloket := "SELECT @id:=@id+1 AS id,nosamb,nama,alamat,COALESCE(nohp,'') AS nohp,COALESCE(kodekolektif,'') AS kodekolektif,COALESCE(`status`,'') AS `status`,flag,COALESCE(kodekondisimeter,'') AS kodekondisimeter,kodegol,kodeblok,koderayon,COALESCE(kodekelurahan,'') AS kodekelurahan FROM pelanggan,(SELECT @id:=0) AS id WHERE flaghapus=0 AND YEAR(waktuupdate)>= ? "
		pelanggan_loket, e_pelloket := dbloket.QueryContext(ctx_pelloket, q_pelloket, tahun_update)
		if e_pelloket != nil {
			log.Fatal("Gagal ambil data : ", e_pelloket)
		}

		defer pelanggan_loket.Close()

		for pelanggan_loket.Next() {
			var pelanggan DataPelLoket
			e_pelanggan := pelanggan_loket.Scan(&pelanggan.Id, &pelanggan.Nosamb, &pelanggan.Nama, &pelanggan.Alamat, &pelanggan.Nohp, &pelanggan.Kodekolektif, &pelanggan.Status, &pelanggan.Flag, &pelanggan.Kodekondisimeter, &pelanggan.kodegol, &pelanggan.Kodeblok, &pelanggan.Koderayon, &pelanggan.Kodekelurahan)
			if e_pelanggan != nil {
				log.Fatal("Data kosong :", e_pelanggan)
			}
			dataPelLoket = append(dataPelLoket, pelanggan)
			//fmt.Println("Sebelum ", pelanggan.Id, "-", pelanggan.Nosamb, "-", pelanggan.Nama, "-", pelanggan.Keterangan)
		}

		defer pelanggan_loket.Close()
		fmt.Println(" [DONE]")
		// END AMBIL PELANGGAN BILLING

		for {
			dbbilling := connmaster.ConnBilling(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)
			defer dbbilling.Close()

			// Mengecek koneksi ke database
			err := dbbilling.Ping()
			if err != nil {
				fmt.Println("Tidak dapat terhubung ke database:", err)
				// pesan = "Tidak dapat terhubung ke database:" + err.Error()
				// telebot.TeleBot(pesan)
				goto SleepB1
			} else {

				break

			}
		SleepB1:
			time.Sleep(5 * time.Second)
		}

		//  CEK KONEKSI LOKET
		dbbill := connmaster.ConnBilling(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)
		defer dbbill.Close()

		// Mengecek koneksi ke database Loket
		err = dbbill.Ping()
		if err != nil {
			log.Fatalf("Tidak dapat terhubung ke database: %v", err)
		}

		var nosambLoket []string
		for _, pelanggan := range dataPelLoket {
			nosambLoket = append(nosambLoket, pelanggan.Nosamb)

		}

		fmt.Print("AMBIL PELANGGAN BILLING")
		// AMBIL PELANGGAN BILLING
		ctx_pelbill := context.Background()
		q_pelbill := fmt.Sprintf("SELECT @id:=@id+1 AS id,nosamb,nama,alamat,COALESCE(nohp,'') AS nohp,COALESCE(kodekolektif,'') AS kodekolektif,COALESCE(`status`,'') AS `status`,flag,COALESCE(kodekondisimeter,'') AS kodekondisimeter,kodegol,kodeblok,koderayon,COALESCE(kodekelurahan,'') AS kodekelurahan FROM pelanggan,(SELECT @id:=0) AS id WHERE nosamb IN ('%s') AND flaghapus=0", strings.Join(nosambLoket, "','"))
		pelanggan_bill, e_pelbill := dbbill.QueryContext(ctx_pelbill, q_pelbill)
		if e_pelbill != nil {
			log.Fatal("Gagal ambil data : ", e_pelbill)
		}

		defer pelanggan_bill.Close()

		var dataPelbill []DataPelBill

		for pelanggan_bill.Next() {
			var pelanggan DataPelBill
			e_pelanggan := pelanggan_bill.Scan(&pelanggan.Id, &pelanggan.Nosamb, &pelanggan.Nama, &pelanggan.Alamat, &pelanggan.Nohp, &pelanggan.Kodekolektif, &pelanggan.Status, &pelanggan.Flag, &pelanggan.Kodekondisimeter, &pelanggan.kodegol, &pelanggan.Kodeblok, &pelanggan.Koderayon, &pelanggan.Kodekelurahan)
			if e_pelanggan != nil {
				log.Fatal("Data kosong :", e_pelanggan)
			}
			dataPelbill = append(dataPelbill, pelanggan)
			// fmt.Println("PELANGGAN BILL : ", pelanggan.Id, "-", pelanggan.Nosamb, "-", pelanggan.Nama, "-", pelanggan.Nohp)
		}
		fmt.Println(" [DONE]")
		fmt.Print("BANDINGKAN PERUBAHAN PELANGGAN LOKET - BILLING")
		// BANDINGKAN PERUBAHAN PELANGGAN LOKET - BILLING
		for _, pelangganBill := range dataPelbill {
			for i, pelangganLok := range dataPelLoket {
				if pelangganLok.Nosamb == pelangganBill.Nosamb {
					if pelangganLok.Nama != pelangganBill.Nama {
						dataPelLoket[i].Keterangan = "Terdapat perubahan yang belum terupdate ke Billing! => Nama Loket : " + pelangganLok.Nama + " Billing : " + pelangganBill.Nama
					} else if pelangganLok.Alamat != pelangganBill.Alamat {
						dataPelLoket[i].Keterangan = "Terdapat perubahan yang belum terupdate ke Billing! => Alamat Loket : " + pelangganLok.Alamat + " Billing : " + pelangganBill.Alamat
					} else if pelangganLok.Nohp != pelangganBill.Nohp {
						dataPelLoket[i].Keterangan = "Terdapat perubahan yang belum terupdate ke Billing! => Nohp Loket : " + pelangganLok.Nohp + " Billing : " + pelangganBill.Nohp
					} else if pelangganLok.Kodekolektif != pelangganBill.Kodekolektif {
						dataPelLoket[i].Keterangan = "Terdapat perubahan yang belum terupdate ke Billing! => Kodekolektif Loket : " + pelangganLok.Kodekolektif + " Billing : " + pelangganBill.Kodekolektif
					} else if pelangganLok.Status != pelangganBill.Status {
						dataPelLoket[i].Keterangan = "Terdapat perubahan yang belum terupdate ke Billing! => Status Loket : " + pelangganLok.Status + " Billing : " + pelangganBill.Status
					} else if pelangganLok.Flag != pelangganBill.Flag {
						dataPelLoket[i].Keterangan = "Terdapat perubahan yang belum terupdate ke Billing! => Flag Loket : " + pelangganLok.Flag + " Billing : " + pelangganBill.Flag
					} else if pelangganLok.Kodekondisimeter != pelangganBill.Kodekondisimeter {
						dataPelLoket[i].Keterangan = "Terdapat perubahan yang belum terupdate ke Billing! => Kodekondisimeter Loket : " + pelangganLok.Kodekondisimeter + "Billing : " + pelangganBill.Kodekondisimeter
					} else if pelangganLok.kodegol != pelangganBill.kodegol {
						dataPelLoket[i].Keterangan = "Terdapat perubahan yang belum terupdate ke Billing! => kodegol Loket : " + pelangganLok.kodegol + " Billing : " + pelangganBill.kodegol
					} else if pelangganLok.Kodeblok != pelangganBill.Kodeblok {
						dataPelLoket[i].Keterangan = "Terdapat perubahan yang belum terupdate ke Billing! => Kodeblok Loket : " + pelangganLok.Kodeblok + " Billing : " + pelangganBill.Kodeblok
					} else if pelangganLok.Koderayon != pelangganBill.Koderayon {
						dataPelLoket[i].Keterangan = "Terdapat perubahan yang belum terupdate ke Billing! => Koderayon Loket : " + pelangganLok.Koderayon + " Billing : " + pelangganBill.Koderayon
					} else if pelangganLok.Kodekelurahan != pelangganBill.Kodekelurahan {
						dataPelLoket[i].Keterangan = "Terdapat perubahan yang belum terupdate ke Billing! => Kodekelurahan Loket : " + pelangganLok.Kodekelurahan + " Billing : " + pelangganBill.Kodekelurahan
					}
					// fmt.Println("Sama! Lama :", pelanggans.Nama, "Ket : ", pelanggans.Keterangan, " Berubah nama ", nama, "Ket ", keterangan)
					break
				}

			}
		}
		fmt.Println(" [DONE]")
		// END BANDINGKAN PERUBAHAN PELANGGAN LOKET - BILLING

	}

	fmt.Print("INSERT PESAN PERUBAHAN PELANGGAN")
	var result, pesan2 string
	pesan = pesan + "\nCEK PERUBAHAN PELANGGAN LOKET - BILLING \n \n"
	for _, pelanggan2 := range dataPelLoket {
		if pelanggan2.Keterangan != "" {
			//fmt.Println("Sesudah ", pelanggan2.Id, "-", pelanggan2.Nosamb, "-", pelanggan2.Nama, "-", pelanggan2.Nohp, "-", pelanggan2.Keterangan)
			result = fmt.Sprintf("%s - %s - %s\n", pelanggan2.Nosamb, pelanggan2.Nama, pelanggan2.Keterangan)
		}
	}

	if result == "" {
		pesan = pesan + "TIDAK ADA PERUBAHAN\n"
		pesan2 = " => TIDAK ADA PERUBAHAN PELANGGAN"
	} else {
		pesan = pesan + result
		pesan2 = " => TERDAPAT PERUBAHAN PELANGGAN"
	}
	fmt.Println(pesan2 + " [DONE]")
	defer rows.Close()
	return pesan
}

func CekLPP(DBUser, DBPass, DBName, DBHost, DBPort, IdPDAM, pesan string) string {
	pesan = pesan + "\nCEK LPP LOKET - BILLING \n"

	tahun_ini := time.Now().Year()
	tahun_lalu := time.Now().Year() //- 1
	bulan := time.Now()
	// bulan1 := bulan.Format("01")
	// periode := fmt.Sprintf("%d%s", tahun_ini, bulan1)
	sekarang := bulan.Format("2006-01-02")

	// Ganti nilai-nilai berikut sesuai dengan informasi koneksi MySQL Anda
	dbUser := DBUser
	dbPass := DBPass
	dbName := DBName
	dbHost := DBHost
	dbPort := DBPort
	idPDAM := IdPDAM

	for tahun := tahun_lalu; tahun <= tahun_ini; tahun++ {
		fmt.Println("\n- CEK LPP ", tahun)
		for {
			db := connmaster.ConnMaster(dbUser, dbPass, dbName, dbHost, dbPort)
			defer db.Close()

			// Mengecek koneksi ke database
			err := db.Ping()
			if err != nil {
				fmt.Println("Tidak dapat terhubung ke database:", err)
				// pesan = "Tidak dapat terhubung ke database:" + err.Error()
				// telebot.TeleBot(pesan)
				goto Sleep
			} else {

				break

			}
		Sleep:
			time.Sleep(5 * time.Second)
		}

		// START CEK PELANGGAN BILLING
		db := connmaster.ConnMaster(dbUser, dbPass, dbName, dbHost, dbPort)
		defer db.Close()

		ctx := context.Background()
		//var db *sql.DB
		script := "SELECT idpdam,nama_pdam,`database` FROM sipintar_pdam_config WHERE idpdam = ? "
		rows, err := db.QueryContext(ctx, script, idPDAM)
		if err != nil {
			log.Fatal("Gagal ambil data : ", err)
			//goto Sleep2
		}

		//var dataPelLoket []DataPelLoket
		var Ip_bill, User_bill, Pass_bill, DB_bill, Port_bill string
		var Ip_loket, User_loket, Pass_loket, DB_loket, Port_loket string

		for rows.Next() {
			var idpdam, nama_pdam, database string
			err := rows.Scan(&idpdam, &nama_pdam, &database)
			if err != nil {
				log.Fatalf("Data kosong : %v", err)
			}

			//Menampung Array hasil select config database
			databs, err := readJSONArray(database)
			if err != nil {
				log.Fatal("Gagal membaca data :", err)
			}

			for _, data := range databs {

				if data.Tipe == "billing" {
					Ip_bill = data.DBHost
					User_bill = data.DBUser
					Pass_bill = data.DBPass
					DB_bill = data.DBName
					Port_bill = data.DBPort

				}
				if data.Tipe == "loket" {
					Ip_loket = data.DBHost
					User_loket = data.DBUser
					Pass_loket = data.DBPass
					DB_loket = data.DBName
					Port_loket = data.DBPort

				}

			}
		}

		fmt.Println("1. CEK LPP AIR")
		// START CEK LPP AIR
		// dbloket := CekConnLoket(Ip_loket, User_loket, Pass_loket, DB_loket, Port_loket)

		//  CEK KONEKSI LOKET
		dbloket := connmaster.ConnLoket(User_loket, Pass_loket, DB_loket, Ip_loket, Port_loket)
		defer dbloket.Close()

		// Mengecek koneksi ke database Loket
		err = dbloket.Ping()
		if err != nil {
			log.Fatalf("Tidak dapat terhubung ke database: %v", err)
		}

		for {
			dbloket := connmaster.ConnLoket(User_loket, Pass_loket, DB_loket, Ip_loket, Port_loket)
			defer dbloket.Close()

			// Mengecek koneksi ke database
			err := dbloket.Ping()
			if err != nil {
				fmt.Println("Tidak dapat terhubung ke database:", err)
				// pesan = "Tidak dapat terhubung ke database:" + err.Error()
				// telebot.TeleBot(pesan)
				goto SleepL1
			} else {

				break

			}
		SleepL1:
			time.Sleep(5 * time.Second)
		}

		//  CEK KONEKSI LOKET
		dbbilling := connmaster.ConnBilling(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)
		defer dbbilling.Close()

		// Mengecek koneksi ke database Loket
		err = dbbilling.Ping()
		if err != nil {
			log.Fatalf("Tidak dapat terhubung ke database: %v", err)
		}

		for {
			dbbilling := connmaster.ConnBilling(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)
			defer dbbilling.Close()

			// Mengecek koneksi ke database
			err := dbbilling.Ping()
			if err != nil {
				fmt.Println("Tidak dapat terhubung ke database:", err)
				// pesan = "Tidak dapat terhubung ke database:" + err.Error()
				// telebot.TeleBot(pesan)
				goto SleepB1
			} else {

				break

			}
		SleepB1:
			time.Sleep(5 * time.Second)
		}

		fmt.Print("CEK TABEL BAYAR TAHUN BILLING")
		ctx_cektabel := context.Background()
		qlpp_cektabel := fmt.Sprint("SHOW TABLES LIKE 'bayar", tahun, "%'")
		cek_tabel, e_cektabel := dbbilling.QueryContext(ctx_cektabel, qlpp_cektabel)
		if e_cektabel != nil {
			log.Fatalf("Gagal ambil data : %v", e_cektabel)
		}

		defer cek_tabel.Close()

		var tabel_ada string
		if cek_tabel.Next() {
			tabel_ada = "1"
		} else {
			tabel_ada = "0"
		}

		if tabel_ada == "1" {

			fmt.Println(" -> TABEL bayar", tahun, " TERSEDIA [DONE]")

			fmt.Print("AMBIL LPP AIR LOKET PER TANGGAL")
			ctx_lpploket := context.Background()

			// qlpp_loket := "SELECT @id:=@id+1 AS id,g.* FROM(SELECT DATE(tglbayar) AS tglbayar,COUNT(*) AS jumlah,SUM(rekair) AS rekair,CONCAT('Semua data belum masuk ke billing [Sebanyak : ',COUNT(*),']') AS keterangan FROM bayar WHERE DATE_FORMAT(tglbayar,'%Y%m')=? AND DATE(tglbayar)<? AND flaglunas=1 and flagbatal=0 AND flagangsur=0 AND flag<>4 AND COALESCE(noangsuran,'')='' GROUP BY DATE(tglbayar)) g,(SELECT @id:=0) AS id"
			// lpp_loket, e_lpploket := dbloket.QueryContext(ctx_lpploket, qlpp_loket, periode, sekarang)
			qlpp_loket := "SELECT @id:=@id+1 AS id,g.* FROM(SELECT DATE(tglbayar) AS tglbayar,COUNT(*) AS jumlah,SUM(rekair) AS rekair,CONCAT('Semua data belum masuk ke billing [Sebanyak : ',COUNT(*),']') AS keterangan FROM bayar WHERE YEAR(tglbayar)=? AND DATE(tglbayar)<? AND flaglunas=1 and flagbatal=0 AND flagangsur=0 AND flag<>4 AND (COALESCE(noangsuran,'')='' OR COALESCE(noangsuran,'')='-') GROUP BY DATE(tglbayar)) g,(SELECT @id:=0) AS id"
			lpp_loket, e_lpploket := dbloket.QueryContext(ctx_lpploket, qlpp_loket, tahun, sekarang)
			if e_lpploket != nil {
				log.Fatalf("Gagal ambil data : %v", e_lpploket)
			}

			defer lpp_loket.Close()

			// TAMPUNG LPP LOKET PER TANGGAL
			var tglLppLoket []TglLppLoket
			for lpp_loket.Next() {
				var lpp TglLppLoket
				e_lpp := lpp_loket.Scan(&lpp.Id, &lpp.TglBayar, &lpp.Jumlah, &lpp.Rekair, &lpp.Keterangan)
				if e_lpp != nil {
					log.Fatal("Data kosong :", e_lpp)
				}
				tglLppLoket = append(tglLppLoket, lpp)

			}
			// fmt.Println("LPP AIR LOKET PER TANGGAL")
			// fmt.Println(tglLppLoket)
			defer lpp_loket.Close()
			fmt.Println(" [DONE]")

			fmt.Print("AMBIL LPP AIR BILLING PER TANGGAL")
			ctx_lppbilling := context.Background()
			// qlpp_billing := fmt.Sprint("SELECT DATE(tglbayar) AS tglbayar,COUNT(*) AS jumlah,SUM(rekair) AS rekair FROM bayar", tahun, " WHERE DATE_FORMAT(tglbayar,'%Y%m')=? AND flag<>4 AND flaglunas=1 and flagbatal=0 AND flagangsur=0 AND COALESCE(noangsuran,'')='' AND DATE(tglbayar)<? GROUP BY DATE(tglbayar)")
			// lpp_bill, e_lppbilling := dbbilling.QueryContext(ctx_lppbilling, qlpp_billing, tahun, sekarang)
			qlpp_billing := fmt.Sprint("SELECT DATE(tglbayar) AS tglbayar,COUNT(*) AS jumlah,SUM(rekair) AS rekair FROM bayar", tahun, " WHERE YEAR(tglbayar)=? AND flag<>4 AND flaglunas=1 and flagbatal=0 AND flagangsur=0 AND (COALESCE(noangsuran,'')='' OR COALESCE(noangsuran,'')='-') AND DATE(tglbayar)<? GROUP BY DATE(tglbayar)")
			lpp_bill, e_lppbilling := dbbilling.QueryContext(ctx_lppbilling, qlpp_billing, tahun, sekarang)
			if e_lppbilling != nil {
				log.Fatalf("Gagal ambil data : %v", e_lppbilling)
			}

			defer lpp_bill.Close()

			// TAMPUNG LPP BILLING PER TANGGAL
			var tgllppbilling []TglLppBill
			for lpp_bill.Next() {
				var lpp TglLppBill
				e_lppbill := lpp_bill.Scan(&lpp.TglBayar, &lpp.Jumlah, &lpp.Rekair)
				if e_lppbill != nil {
					log.Fatal("Data kosong :", e_lppbill)
				}
				tgllppbilling = append(tgllppbilling, lpp)

			}

			fmt.Println(" [DONE]")
			defer lpp_bill.Close()

			fmt.Print("BANDINGKAN LPP AIR LOKET - BILLING PER TANGGAL")
			// CEK LPP LOKET - BILLING PER TANGGAL
			for i, datatglLoket := range tglLppLoket {
				for _, datatglBill := range tgllppbilling {
					if datatglBill.TglBayar == datatglLoket.TglBayar {
						if datatglBill.Jumlah != datatglLoket.Jumlah {
							tglLppLoket[i].Keterangan = fmt.Sprintf("Terdapat selisih jumlah data di tanggal penerimaan ini => Billing : %d Loket : %d", datatglBill.Jumlah, datatglLoket.Jumlah)
						} else if int(datatglBill.Rekair) != int(datatglLoket.Rekair) {
							tglLppLoket[i].Keterangan = fmt.Sprintf("Terdapat selisih rekair di tanggal penerimaan ini => Billing : %.2f Loket : %.2f", datatglBill.Rekair, datatglLoket.Rekair)
						} else {
							tglLppLoket[i].Keterangan = ""
						}
					}
				}
			}
			//  END CEK LPP LOKET - BILLING PER TANGGAL
			fmt.Println(" [DONE]")

			// CEK RINCIAN LPP PER TANGGAL BERMASALAH
			filter := func(data TglLppLoket) bool {
				return data.Keterangan != ""
			}
			fmt.Print("FILTER LPP AIR LOKET - BILLING PER TANGGAL SELISIH")
			var tglSelisih []string
			for _, data2 := range tglLppLoket {
				if filter(data2) {
					// tglSelisih = append(tglSelisih, data2)
					tglSelisih = append(tglSelisih, data2.TglBayar)
				}
			}
			fmt.Println(" [DONE]")

			fmt.Print("AMBIL LPP AIR LOKET PER KODE")
			ctx_lppkode := context.Background()
			qlpp_kode := fmt.Sprintf("SELECT @id:=@id+1 AS id,kode,DATE(tglbayar) AS tglbayar,rekair,'Data berikut belum masuk ke billing' AS keterangan FROM bayar,(SELECT @id:=0) AS id WHERE DATE(tglbayar) IN ('%s') AND flag<>4 AND flaglunas=1 and flagbatal=0 AND flagangsur=0 AND (COALESCE(noangsuran,'')='' OR COALESCE(noangsuran,'')='-') ORDER BY tglbayar,kode ASC", strings.Join(tglSelisih, "','"))
			lpp_kode, e_lppkode := dbloket.QueryContext(ctx_lppkode, qlpp_kode)
			if e_lppkode != nil {
				log.Fatalf("Gagal ambil data : %v", e_lppkode)
			}

			defer lpp_kode.Close()
			// TAMPUNG LPP LOKET PER KODE
			var dataLppLoket []DataLppLoket
			for lpp_kode.Next() {
				var lpp DataLppLoket
				e_lpp := lpp_kode.Scan(&lpp.Id, &lpp.Kode, &lpp.TglBayar, &lpp.Rekair, &lpp.Keterangan)
				if e_lpp != nil {
					log.Fatal("Data kosong :", e_lpp)
				}
				dataLppLoket = append(dataLppLoket, lpp)

			}
			fmt.Println(" [DONE]")
			defer lpp_kode.Close()
			// fmt.Println("LPP AIR LOKET PER KODE")
			// fmt.Println(dataLppLoket)
			fmt.Print("AMBIL LPP AIR BILLING PER KODE")
			ctx_lppkode2 := context.Background()
			qlpp_kode2 := fmt.Sprintf("SELECT @id:=@id+1 AS id,kode,DATE(tglbayar) AS tglbayar,rekair FROM bayar%d,(SELECT @id:=0) AS id WHERE DATE(tglbayar) IN ('%s') AND flag<>4 AND flaglunas=1 and flagbatal=0 AND flagangsur=0 AND (COALESCE(noangsuran,'')='' OR COALESCE(noangsuran,'')='-') ORDER BY tglbayar,kode ASC", tahun, strings.Join(tglSelisih, "','"))
			lpp_kode2, e_lppkode2 := dbbilling.QueryContext(ctx_lppkode2, qlpp_kode2)
			if e_lppkode2 != nil {
				log.Fatalf("Gagal ambil data : %v", e_lppkode2)
			}

			defer lpp_kode2.Close()
			// TAMPUNG LPP LOKET PER KODE
			var dataLppLoket2 []DataLppBill
			for lpp_kode2.Next() {
				var lpp2 DataLppBill
				e_lpp2 := lpp_kode2.Scan(&lpp2.Id, &lpp2.Kode, &lpp2.TglBayar, &lpp2.Rekair)
				if e_lpp2 != nil {
					log.Fatal("Data kosong :", e_lpp2)
				}
				dataLppLoket2 = append(dataLppLoket2, lpp2)

			}
			fmt.Println(" [DONE]")
			defer lpp_kode2.Close()
			// fmt.Println("LPP KODE BILLING ADA")
			// fmt.Println(dataLppLoket2)
			fmt.Print("BANDINGKAN LPP AIR LOKET DAN BILLING PER KODE")
			// BANDINGKAN LPP LOKET DAN BILLING
			for _, kodeBill := range dataLppLoket2 {
				for i, kodeLoket := range dataLppLoket {
					if kodeBill.Kode == kodeLoket.Kode {
						if kodeBill.Rekair == kodeLoket.Rekair {
							dataLppLoket[i].Keterangan = ""
						} else if kodeBill.Rekair != kodeLoket.Rekair {
							dataLppLoket[i].Keterangan = fmt.Sprintf("Selisih rekair untuk rekening ini => Billing : %.2f Loket : %.2f", kodeBill.Rekair, kodeLoket.Rekair)
						}
					}
				}
			}
			fmt.Println(" [DONE]")
			// END CEK RINCIAN LPP PER TANGGAL BERMASALAH

			fmt.Print("INSERT PESAN SELISIH LPP AIR LOKET - BILLING")
			var result, pesan2 string
			pesan = pesan + "\nLPP TAHUN " + strconv.Itoa(tahun) + "\n"
			pesan = pesan + "\n1. LPP Air \n\n"
			for _, dtglLppLoket := range tglLppLoket {
				if dtglLppLoket.Keterangan != "" {
					//result = fmt.Sprintf("%s,KODE,%s\n", dtglLppLoket.TglBayar, dtglLppLoket.Keterangan)
					pesan = pesan + fmt.Sprintf("%s,'',%s\n", dtglLppLoket.TglBayar, dtglLppLoket.Keterangan)
				}
				for _, dataLpp := range dataLppLoket {
					if dtglLppLoket.TglBayar == dataLpp.TglBayar {
						if dataLpp.Keterangan != "" {
							result = fmt.Sprintf("%s,%s,%s\n", dataLpp.TglBayar, dataLpp.Kode, dataLpp.Keterangan)
							pesan = pesan + fmt.Sprintf("%s,%s,%s\n", dataLpp.TglBayar, dataLpp.Kode, dataLpp.Keterangan)
						}
					}
				}
			}
			if result == "" {
				pesan = pesan + "TIDAK ADA SELISIH PENERIMAAN AIR\n"
				pesan2 = " => TIDAK ADA SELISIH PENERIMAAN AIR"
			} else {
				pesan = pesan + ""
				pesan2 = " => TERDAPAT SELISIH PENERIMAAN AIR"
			}
			fmt.Println(pesan2 + " [DONE]")
		} else {
			pesan = pesan + fmt.Sprintf("TABEL bayar %d TIDAK DITEMUKAN", tahun)
		}

		fmt.Println("\n2. CEK LPP ANGS AIR")
		fmt.Print("CEK TABEL BAYAR TAHUN BILLING")

		if tabel_ada == "1" {

			fmt.Println(" -> TABEL bayar", tahun, " TERSEDIA [DONE]")

			fmt.Print("AMBIL LPP ANGS AIR LOKET PER TANGGAL")
			// CEK LPP ANGS. AIR LOKET PER TANGGAL
			ctx_angsloket := context.Background()
			// qlpp_angsloket := "SELECT @id:=@id+1 AS id,g.* FROM(SELECT DATE(waktubayar) AS tglbayar,COUNT(*) AS jumlah,SUM(total-dendatunggakan) AS total,CONCAT('Semua data belum masuk ke billing [Sebanyak : ',COUNT(*),']') AS keterangan FROM nonair WHERE DATE_FORMAT(waktubayar,'%Y%m')=? AND flaglunas=1 AND flagbatal=0 AND flaghapus=0 AND COALESCE(noangsuran,'')<>'' AND flagangsur=1 AND jenis IN ('JNS-36','JNS-37') AND DATE(waktubayar)<? GROUP BY DATE(waktubayar)) g,(SELECT @id:=0) AS id"
			// lppangs_loket, e_angsloket := dbloket.QueryContext(ctx_angsloket, qlpp_angsloket, periode, sekarang)
			qlpp_angsloket := "SELECT @id:=@id+1 AS id,g.* FROM(SELECT DATE(waktubayar) AS tglbayar,COUNT(*) AS jumlah,SUM(total-dendatunggakan) AS total,CONCAT('Semua data belum masuk ke billing [Sebanyak : ',COUNT(*),']') AS keterangan FROM nonair WHERE YEAR(waktubayar)=? AND flaglunas=1 AND flagbatal=0 AND flaghapus=0 AND COALESCE(noangsuran,'')<>'' AND flagangsur=1 AND jenis IN ('JNS-36','JNS-37') AND DATE(waktubayar)<? GROUP BY DATE(waktubayar)) g,(SELECT @id:=0) AS id"
			lppangs_loket, e_angsloket := dbloket.QueryContext(ctx_angsloket, qlpp_angsloket, tahun, sekarang)
			if e_angsloket != nil {
				log.Fatalf("Gagal ambil data : %v", e_angsloket)
			}

			defer lppangs_loket.Close()
			fmt.Println(" [DONE]")
			// TAMPUNG LPP ANGS. LOKET PER TANGGAL
			var tglangsloket []TglAngsLoket
			for lppangs_loket.Next() {
				var lpp_angs TglAngsLoket
				e_lpp := lppangs_loket.Scan(&lpp_angs.Id, &lpp_angs.TglBayar, &lpp_angs.Jumlah, &lpp_angs.Rekair, &lpp_angs.Keterangan)
				if e_lpp != nil {
					log.Fatal("Data kosong :", e_lpp)
				}
				tglangsloket = append(tglangsloket, lpp_angs)

			}

			defer lppangs_loket.Close()
			//fmt.Println(tglangsloket)
			// END CEK LPP ANGS. AIR LOKET PER TANGGAL

			fmt.Print("AMBIL LPP ANGS AIR BILLING PER TANGGAL")
			// CEK LPP ANGS. AIR BILLIG PER TANGGAL
			ctx_AngsBilling := context.Background()
			// qlpp_angsbilling := fmt.Sprint("SELECT DATE(tglbayar) AS tglbayar,COUNT(*) AS jumlah,SUM(rekair) AS rekair FROM bayar", tahun, " WHERE DATE_FORMAT(tglbayar,'%Y%m')=? AND flaglunas=1 AND flagbatal=0 AND COALESCE(noangsuran,'')<>'' AND DATE(tglbayar)<? GROUP BY DATE(tglbayar)")
			// lppangs_bill, e_AngsBilling := dbbilling.QueryContext(ctx_AngsBilling, qlpp_angsbilling, periode, sekarang)
			qlpp_angsbilling := fmt.Sprint("SELECT DATE(tglbayar) AS tglbayar,COUNT(*) AS jumlah,SUM(rekair) AS rekair FROM bayar", tahun, " WHERE YEAR(tglbayar)=? AND flaglunas=1 AND flagbatal=0 AND COALESCE(noangsuran,'')<>'' AND DATE(tglbayar)<? GROUP BY DATE(tglbayar)")
			lppangs_bill, e_AngsBilling := dbbilling.QueryContext(ctx_AngsBilling, qlpp_angsbilling, tahun, sekarang)
			if e_AngsBilling != nil {
				log.Fatalf("Gagal ambil data : %v", e_AngsBilling)
			}

			defer lppangs_bill.Close()
			fmt.Println(" [DONE]")
			// TAMPUNG LPP ANGS. BILLING PER TANGGAL
			var tglAngsBilling []TglAngsBill
			for lppangs_bill.Next() {
				var lpp TglAngsBill
				e_lpp := lppangs_bill.Scan(&lpp.TglBayar, &lpp.Jumlah, &lpp.Rekair)
				if e_lpp != nil {
					log.Fatal("Data kosong :", e_lpp)
				}
				tglAngsBilling = append(tglAngsBilling, lpp)

			}
			defer lppangs_bill.Close()
			// END CEK LPP ANGS. AIR BILLIG PER TANGGAL

			fmt.Print("BANDINGKAN LPP ANGS AIR LOKET - BILLING PER TANGGAL")
			// CEK LPP ANGS. LOKET - BILLING PER TANGGA
			for i, datatglLoket := range tglangsloket {
				for _, datatglBill := range tglAngsBilling {
					if datatglBill.TglBayar == datatglLoket.TglBayar {
						if datatglBill.Jumlah != datatglLoket.Jumlah {
							tglangsloket[i].Keterangan = fmt.Sprintf("Terdapat selisih jumlah data di tanggal penerimaan ini, Billing : %d Loket : %d", datatglBill.Jumlah, datatglLoket.Jumlah)
						} else if int(datatglBill.Rekair) != int(datatglLoket.Rekair) {
							tglangsloket[i].Keterangan = fmt.Sprintf("Terdapat selisih rekair di tanggal penerimaan ini, Billing : %.2f Loket : %.2f", datatglBill.Rekair, datatglLoket.Rekair)
						} else {
							tglangsloket[i].Keterangan = ""
						}
					}
				}
			}
			//  END CEK LPP ANGS. LOKET - BILLING PER TANGGAL
			fmt.Println(" [DONE]")

			// CEK RINCIAN LPP PER TANGGAL BERMASALAH
			filter_angs := func(data_angs TglAngsLoket) bool {
				return data_angs.Keterangan != ""
			}
			fmt.Print("FILTER LPP ANGS LOKET - BILLING PER TANGGAL SELISIH")
			var tglAngsSelisih []string
			for _, data2_angs := range tglangsloket {
				if filter_angs(data2_angs) {
					// tglSelisih = append(tglSelisih, data2)
					tglAngsSelisih = append(tglAngsSelisih, data2_angs.TglBayar)
				}
			}
			fmt.Println(" [DONE]")

			fmt.Print("AMBIL LPP ANGS AIR LOKET PER KODE")
			clpp_angskode := context.Background()
			qlpp_angskode := fmt.Sprintf("SELECT @id:=@id+1 AS id,urutan AS kode,jenis,DATE(waktubayar) AS tglbayar,(total-dendatunggakan) AS total,'Data berikut belum masuk ke billing' AS keterangan FROM nonair,(SELECT @id:=0) AS id WHERE DATE(waktubayar) IN ('%s') AND flaglunas=1 AND flagbatal=0 AND flaghapus=0 AND COALESCE(noangsuran,'')<>'' AND flagangsur=1 AND jenis IN ('JNS-36','JNS-37') ORDER BY waktubayar,kode ASC", strings.Join(tglAngsSelisih, "','"))
			lpp_angskode, e_lppangskode := dbloket.QueryContext(clpp_angskode, qlpp_angskode)
			if e_lppangskode != nil {
				log.Fatalf("Gagal ambil data : %v", e_lppangskode)
			}

			defer lpp_angskode.Close()

			var dataAngsLoket []DataAngsLoket
			for lpp_angskode.Next() {
				var angsAL DataAngsLoket
				e_lpp := lpp_angskode.Scan(&angsAL.Id, &angsAL.Kode, &angsAL.Jenis, &angsAL.TglBayar, &angsAL.Rekair, &angsAL.Keterangan)
				if e_lpp != nil {
					log.Fatal("Data kosong :", e_lpp)
				}
				dataAngsLoket = append(dataAngsLoket, angsAL)

			}
			defer lpp_angskode.Close()
			fmt.Println(" [DONE]")

			fmt.Print("AMBIL LPP ANGS AIR BILLING PER KODE")
			clpp_angskode2 := context.Background()
			qlpp_angskode2 := fmt.Sprintf("SELECT kode,IF(flag=4,'JNS-37','JNS-36') AS jenis,DATE(tglbayar) AS tglbayar,rekair FROM bayar%d WHERE DATE(tglbayar) IN ('%s') AND flaglunas=1 and flagbatal=0 AND flagangsur=0 AND COALESCE(noangsuran,'')<>'' ORDER BY tglbayar,kode ASC", tahun, strings.Join(tglAngsSelisih, "','"))
			lpp_angskode2, err := dbbilling.QueryContext(clpp_angskode2, qlpp_angskode2)
			if err != nil {
				log.Fatalf("Gagal ambil data : %v", err)
			}
			defer lpp_angskode2.Close()

			var dataAngsBill []DataAngsBill
			for lpp_angskode2.Next() {
				var angsAB DataAngsBill
				e_lpp2 := lpp_angskode2.Scan(&angsAB.Kode, &angsAB.Jenis, &angsAB.TglBayar, &angsAB.Rekair)
				if e_lpp2 != nil {
					log.Fatal("Data kosong :", e_lpp2)
				}
				dataAngsBill = append(dataAngsBill, angsAB)

			}
			defer lpp_angskode2.Close()
			fmt.Println(" [DONE]")

			fmt.Print("BANDINGKAN LPP ANGS AIR LOKET DAN BILLING PER KODE")
			// BANDINGKAN LPP LOKET DAN BILLING
			for _, kodeAngsBill := range dataAngsBill {
				for i, kodeAngsLoket := range dataAngsLoket {
					if kodeAngsBill.Kode == kodeAngsLoket.Kode {
						if kodeAngsBill.Rekair == kodeAngsLoket.Rekair {
							dataAngsLoket[i].Keterangan = ""
						} else if kodeAngsBill.Rekair != kodeAngsLoket.Rekair {
							dataAngsLoket[i].Keterangan = fmt.Sprintf("Selisih rekair untuk rekening ini => Billing : %.2f Loket : %.2f", kodeAngsBill.Rekair, kodeAngsLoket.Rekair)
						}
					}
				}
			}
			fmt.Println(" [DONE]")
			// END CEK RINCIAN LPP PER TANGGAL BERMASALAH

			fmt.Print("INSERT PESAN SELISIH LPP AIR LOKET - BILLING")
			var resultAngs, pesanAngs2 string
			pesan = pesan + "\n2. LPP Angs. Air \n\n"
			for _, dtglangsloket := range tglangsloket {
				if dtglangsloket.Keterangan != "" {
					pesan = pesan + fmt.Sprintf("%s,KODE,JENIS,%s\n", dtglangsloket.TglBayar, dtglangsloket.Keterangan)
				}
				for _, dataAngs := range dataAngsLoket {
					if dtglangsloket.TglBayar == dataAngs.TglBayar {
						if dataAngs.Keterangan != "" {
							resultAngs = fmt.Sprintf("%s,%s,%s,%s\n", dataAngs.TglBayar, dataAngs.Kode, dataAngs.Jenis, dataAngs.Keterangan)
							pesan = pesan + fmt.Sprintf("%s,%s,%s,%s\n", dataAngs.TglBayar, dataAngs.Kode, dataAngs.Jenis, dataAngs.Keterangan)
						}
					}
				}
			}

			if resultAngs == "" {
				pesan = pesan + "TIDAK ADA SELISIH PENERIMAAN ANGS. AIR\n"
				pesanAngs2 = " => TIDAK ADA SELISIH PENERIMAAN ANGS. AIR"
			} else {
				pesan = pesan + ""
				pesanAngs2 = " => TERDAPAT SELISIH PENERIMAAN ANGS. AIR"
			}
			fmt.Println(pesanAngs2 + " [DONE]")
		} else {
			pesan = pesan + fmt.Sprintf("TABEL bayar %d TIDAK DITEMUKAN", tahun)
		}

		fmt.Println("\n3. CEK LPP ANGS NONAIR")
		fmt.Print("AMBIL LPP ANGS NONAIR LOKET")
		// CEK LPP ANGS. AIR LOKET PER TANGGAL
		ctx_tangsnonl := context.Background()
		// q_tangsnonl := "SELECT @id:=@id+1 AS id,g.* FROM(SELECT DATE(waktubayar) AS tglbayar,COUNT(*) AS jumlah,SUM(total) AS total,CONCAT('Semua data belum masuk ke billing [Sebanyak : ',COUNT(*),']') AS keterangan FROM nonair WHERE DATE_FORMAT(waktubayar,'%Y%m')=? AND flaglunas=1 AND flagbatal=0 AND flaghapus=0 AND COALESCE(noangsuran,'')<>'' AND flagangsur=1 AND jenis NOT IN ('JNS-36','JNS-37') AND DATE(waktubayar)<? GROUP BY DATE(waktubayar)) g,(SELECT @id:=0) AS id"
		// tangs_nonl, e_angsloket := dbloket.QueryContext(ctx_tangsnonl, q_tangsnonl, periode, sekarang)
		q_tangsnonl := "SELECT @id:=@id+1 AS id,g.* FROM(SELECT DATE(waktubayar) AS tglbayar,COUNT(*) AS jumlah,SUM(total) AS total,CONCAT('Semua data belum masuk ke billing [Sebanyak : ',COUNT(*),']') AS keterangan FROM nonair WHERE YEAR(waktubayar)=? AND flaglunas=1 AND flagbatal=0 AND flaghapus=0 AND COALESCE(noangsuran,'')<>'' AND flagangsur=1 AND jenis NOT IN ('JNS-36','JNS-37') AND DATE(waktubayar)<? GROUP BY DATE(waktubayar)) g,(SELECT @id:=0) AS id"
		tangs_nonl, e_angsloket := dbloket.QueryContext(ctx_tangsnonl, q_tangsnonl, tahun, sekarang)
		if e_angsloket != nil {
			log.Fatalf("Gagal ambil data : %v", e_angsloket)
		}

		defer tangs_nonl.Close()

		// TAMPUNG LPP ANGS. LOKET PER TANGGAL
		var tglangsnonl []TglAngsNonL
		for tangs_nonl.Next() {
			var lpp_tangsnonl TglAngsNonL
			e_lpp := tangs_nonl.Scan(&lpp_tangsnonl.Id, &lpp_tangsnonl.TglBayar, &lpp_tangsnonl.Jumlah, &lpp_tangsnonl.Total, &lpp_tangsnonl.Keterangan)
			if e_lpp != nil {
				log.Fatal("Data kosong :", e_lpp)
			}
			tglangsnonl = append(tglangsnonl, lpp_tangsnonl)

		}
		defer tangs_nonl.Close()
		fmt.Println(" [DONE]")

		fmt.Print("AMBIL LPP ANGS NONAIR BILLING")

		ctx_tangsnonb := context.Background()
		// q_tangsnonb := fmt.Sprint("SELECT DATE(waktubayar) AS tglbayar,COUNT(*) AS jumlah,SUM(total) AS total FROM nonair WHERE DATE_FORMAT(waktubayar,'%Y%m')=? AND flaglunas=1 AND flagbatal=0 AND flaghapus=0 AND COALESCE(noangsuran,'')<>'' AND flagangsur=1 AND jenis NOT IN ('JNS-36','JNS-37') AND DATE(waktubayar)<? GROUP BY DATE(waktubayar)")
		// tangs_nonb, e_angsbill := dbbilling.QueryContext(ctx_tangsnonb, q_tangsnonb, periode, sekarang)
		tangs_nonb, e_angsbill := dbbilling.QueryContext(ctx_tangsnonb, "SELECT DATE(waktubayar) AS tglbayar,COUNT(*) AS jumlah,SUM(total) AS total FROM nonair WHERE YEAR(waktubayar)=? AND flaglunas=1 AND flagbatal=0 AND flaghapus=0 AND COALESCE(noangsuran,'')<>'' AND flagangsur=1 AND jenis NOT IN ('JNS-36','JNS-37') AND DATE(waktubayar)<? GROUP BY DATE(waktubayar)", tahun, sekarang)
		if e_angsbill != nil {
			log.Fatal("Data kosong :", e_angsbill)
		}
		defer tangs_nonb.Close()

		var tglangsnonb []TglAngsNonB
		for tangs_nonb.Next() {
			var lpp_tangsnonb TglAngsNonB
			e_lpp := tangs_nonb.Scan(&lpp_tangsnonb.TglBayar, &lpp_tangsnonb.Jumlah, &lpp_tangsnonb.Total)
			if e_lpp != nil {
				log.Fatal("Data kosong :", e_lpp)
			}
			tglangsnonb = append(tglangsnonb, lpp_tangsnonb)
		}
		defer tangs_nonb.Close()
		fmt.Println(" [DONE]")

		fmt.Print("BANDINGKAN LPP ANGS NONAIR LOKET - BILLING PER TANGGAL")

		for i, tgl_angsnonl := range tglangsnonl {
			for _, tgl_angsnonb := range tglangsnonb {
				if tgl_angsnonl.TglBayar == tgl_angsnonb.TglBayar {
					if tgl_angsnonl.Jumlah != tgl_angsnonb.Jumlah {
						tglangsnonl[i].Keterangan = fmt.Sprintf("Terdapat selisih jumlah data di tanggal penerimaan ini, Billing : %d Loket : %d", tgl_angsnonb.Jumlah, tgl_angsnonl.Jumlah)
					} else if int(tgl_angsnonl.Total) != int(tgl_angsnonb.Total) {
						tglangsnonl[i].Keterangan = fmt.Sprintf("Terdapat selisih rekair di tanggal penerimaan ini, Billing : %.2f Loket : %.2f", tgl_angsnonb.Total, tgl_angsnonl.Total)
					} else {
						tglangsnonl[i].Keterangan = ""
					}
				}
			}
		}

		fmt.Println(" [DONE]")

		fmt.Print("FILTER LPP ANGS NONAIR LOKET - BILLING PER TANGGAL SELISIH")

		filter_angsnon := func(data_angsnon TglAngsNonL) bool {
			return data_angsnon.Keterangan != ""
		}

		var tglAngsNonSelisih []string
		for _, data2_angsnon := range tglangsnonl {
			if filter_angsnon(data2_angsnon) {
				tglAngsNonSelisih = append(tglAngsNonSelisih, data2_angsnon.TglBayar)
			}
		}

		fmt.Println(" [DONE]")

		fmt.Print("AMBIL LPP ANGS NONAIR LOKET PER KODE")

		ctx_kangsnonl := context.Background()
		q_kangsnonl := fmt.Sprintf("SELECT @id:=@id+1,urutan AS kode,jenis,DATE(waktubayar) AS tglbayar,total,'Data berikut belum masuk ke billing' AS keterangan FROM nonair, (SELECT @id:=0) AS id WHERE DATE(waktubayar) IN ('%s') AND flaglunas=1 AND flagbatal=0 AND flaghapus=0 AND COALESCE(noangsuran,'')<>'' AND flagangsur=1 AND jenis NOT IN ('JNS-36','JNS-37') ORDER BY waktubayar,kode ASC", strings.Join(tglAngsNonSelisih, "','"))
		lpp_angsnonkode, e_lppangsnonkode := dbloket.QueryContext(ctx_kangsnonl, q_kangsnonl)
		if e_lppangsnonkode != nil {
			log.Fatalf("Gagal ambil data : %v", e_lppangsnonkode)
		}
		defer lpp_angsnonkode.Close()

		var dataAngsNonL []DataAngsNonL
		for lpp_angsnonkode.Next() {
			var angsKL DataAngsNonL
			elpp := lpp_angsnonkode.Scan(&angsKL.Id, &angsKL.Kode, &angsKL.Jenis, &angsKL.TglBayar, &angsKL.Total, &angsKL.Keterangan)
			if elpp != nil {
				log.Fatal("Data kosong :", elpp)
			}
			dataAngsNonL = append(dataAngsNonL, angsKL)
		}
		defer lpp_angsnonkode.Close()

		fmt.Println(" [DONE]")

		fmt.Print("AMBIL LPP ANGS NONAIR BILLING PER KODE")

		ctx_kangsnonb := context.Background()
		q_kangsnonb := fmt.Sprintf("SELECT urutan AS kode,jenis,DATE(waktubayar) AS tglbayar,total FROM nonair WHERE DATE(waktubayar) IN ('%s') AND flaglunas=1 AND flagbatal=0 AND flaghapus=0 AND COALESCE(noangsuran,'')<>'' AND flagangsur=1 AND jenis NOT IN ('JNS-36','JNS-37') ORDER BY waktubayar,kode ASC", strings.Join(tglAngsNonSelisih, "','"))
		lpp_angsnonkode2, e_lppangsnonkode2 := dbbilling.QueryContext(ctx_kangsnonb, q_kangsnonb)
		if e_lppangsnonkode2 != nil {
			log.Fatalf("Gagal ambil data : %v", e_lppangsnonkode2)
		}
		defer lpp_angsnonkode2.Close()

		var dataAngsNonB []DataAngsNonB
		for lpp_angsnonkode2.Next() {
			var angsKB DataAngsNonB
			elpp := lpp_angsnonkode2.Scan(&angsKB.Kode, &angsKB.Jenis, &angsKB.TglBayar, &angsKB.Total)
			if elpp != nil {
				log.Fatal("Data kosong :", elpp)
			}
			dataAngsNonB = append(dataAngsNonB, angsKB)
		}
		defer lpp_angsnonkode2.Close()

		fmt.Println(" [DONE]")

		fmt.Print("BANDINGKAN LPP ANGS NONAIR LOKET DAN BILLING PER KODE")

		// BANDINGKAN LPP LOKET DAN BILLING
		for _, kodeAngsNonBill := range dataAngsNonB {
			for i, kodeNonAngsLoket := range dataAngsNonL {
				if kodeAngsNonBill.Kode == kodeNonAngsLoket.Kode {
					if kodeAngsNonBill.Total == kodeNonAngsLoket.Total {
						dataAngsNonL[i].Keterangan = ""
					} else if kodeAngsNonBill.Total != kodeNonAngsLoket.Total {
						dataAngsNonL[i].Keterangan = fmt.Sprintf("Selisih total untuk tagihan nonair ini => Billing : %.2f Loket : %.2f", kodeAngsNonBill.Total, kodeNonAngsLoket.Total)
					}
				}
			}
		}

		fmt.Println(" [DONE]")

		fmt.Print("INSERT PESAN SELISIH LPP AIR LOKET - BILLING")
		var resultAngsNon, pesanAngsNon string
		pesan = pesan + "\n3. LPP Angs. Nonair \n\n"
		for _, dtglangsnonl := range tglangsnonl {
			if dtglangsnonl.Keterangan != "" {
				pesan = pesan + fmt.Sprintf("%s,KODE,%s\n", dtglangsnonl.TglBayar, dtglangsnonl.Keterangan)
			}
			for _, dataAngsNon := range dataAngsNonL {
				if dtglangsnonl.TglBayar == dataAngsNon.TglBayar {
					if dataAngsNon.Keterangan != "" {
						resultAngsNon = fmt.Sprintf("- %s,%s,%s\n", dataAngsNon.TglBayar, dataAngsNon.Kode, dataAngsNon.Keterangan)
						pesan = pesan + fmt.Sprintf("- %s,%s,%s\n", dataAngsNon.TglBayar, dataAngsNon.Kode, dataAngsNon.Keterangan)
					}
				}
			}
		}

		if resultAngsNon == "" {
			pesan = pesan + "TIDAK ADA SELISIH PENERIMAAN ANGS. NONAIR\n"
			pesanAngsNon = " => TIDAK ADA SELISIH PENERIMAAN ANGS. NONAIR"
		} else {
			pesan = pesan + resultAngsNon
			pesanAngsNon = " => TERDAPAT SELISIH PENERIMAAN ANGS. NONAIR"
		}
		fmt.Println(pesanAngsNon + " [DONE]")

		fmt.Println("\n4. CEK LPP NONAIR")
		fmt.Print("AMBIL LPP NONAIR LOKET")
		// CEK LPP . AIR LOKET PER TANGGAL
		ctx_tnonl := context.Background()
		q_tnonl := "SELECT @id:=@id+1 AS id,g.* FROM(SELECT DATE(waktubayar) AS tglbayar,COUNT(*) AS jumlah,SUM(total) AS total,CONCAT('Semua data belum masuk ke billing [Sebanyak : ',COUNT(*),']') AS keterangan FROM nonair WHERE YEAR(waktubayar)=? AND flaglunas=1 AND flagbatal=0 AND flaghapus=0 AND (COALESCE(noangsuran,'')='' OR COALESCE(noangsuran,'')='-') AND flagangsur=0 AND jenis NOT IN ('JNS-36') AND DATE(waktubayar)<? GROUP BY DATE(waktubayar)) g,(SELECT @id:=0) AS id"
		t_nonl, e_loket := dbloket.QueryContext(ctx_tnonl, q_tnonl, tahun, sekarang)
		if e_loket != nil {
			log.Fatalf("Gagal ambil data : %v", e_loket)
		}

		defer t_nonl.Close()

		// TAMPUNG LPP . LOKET PER TANGGAL
		var tglnonl []TglNonL
		for t_nonl.Next() {
			var lpp_tnonl TglNonL
			e_lpp := t_nonl.Scan(&lpp_tnonl.Id, &lpp_tnonl.TglBayar, &lpp_tnonl.Jumlah, &lpp_tnonl.Total, &lpp_tnonl.Keterangan)
			if e_lpp != nil {
				log.Fatal("Data kosong :", e_lpp)
			}
			tglnonl = append(tglnonl, lpp_tnonl)

		}
		defer t_nonl.Close()
		fmt.Println(" [DONE]")

		fmt.Print("AMBIL LPP NONAIR BILLING")

		ctx_tnonb := context.Background()
		t_nonb, e_bill := dbbilling.QueryContext(ctx_tnonb, "SELECT DATE(waktubayar) AS tglbayar,COUNT(*) AS jumlah,SUM(total) AS total FROM nonair WHERE YEAR(waktubayar)=? AND flaglunas=1 AND flagbatal=0 AND flaghapus=0 AND (COALESCE(noangsuran,'')='' OR COALESCE(noangsuran,'')='-') AND flagangsur=0 AND jenis NOT IN ('JNS-36') AND DATE(waktubayar)<? GROUP BY DATE(waktubayar)", tahun, sekarang)
		if e_bill != nil {
			log.Fatal("Data kosong :", e_bill)
		}
		defer t_nonb.Close()

		var tglnonb []TglNonB
		for t_nonb.Next() {
			var lpp_tnonb TglNonB
			e_lpp := t_nonb.Scan(&lpp_tnonb.TglBayar, &lpp_tnonb.Jumlah, &lpp_tnonb.Total)
			if e_lpp != nil {
				log.Fatal("Data kosong :", e_lpp)
			}
			tglnonb = append(tglnonb, lpp_tnonb)
		}
		defer t_nonb.Close()
		fmt.Println(" [DONE]")

		fmt.Print("BANDINGKAN LPP NONAIR LOKET - BILLING PER TANGGAL")

		for i, tgl_nonl := range tglnonl {
			for _, tgl_nonb := range tglnonb {
				if tgl_nonl.TglBayar == tgl_nonb.TglBayar {
					if tgl_nonl.Jumlah != tgl_nonb.Jumlah {
						tglnonl[i].Keterangan = fmt.Sprintf("Terdapat selisih jumlah data di tanggal penerimaan ini, Billing : %d Loket : %d", tgl_nonb.Jumlah, tgl_nonl.Jumlah)
					} else if int(tgl_nonl.Total) != int(tgl_nonb.Total) {
						tglnonl[i].Keterangan = fmt.Sprintf("Terdapat selisih rekair di tanggal penerimaan ini, Billing : %.2f Loket : %.2f", tgl_nonb.Total, tgl_nonl.Total)
					} else {
						tglnonl[i].Keterangan = ""
					}
				}
			}
		}

		fmt.Println(" [DONE]")

		fmt.Print("FILTER LPP NONAIR LOKET - BILLING PER TANGGAL SELISIH")

		filter_non := func(data_non TglNonL) bool {
			return data_non.Keterangan != ""
		}

		var tglNonSelisih []string
		for _, data2_non := range tglnonl {
			if filter_non(data2_non) {
				tglNonSelisih = append(tglNonSelisih, data2_non.TglBayar)
			}
		}

		fmt.Println(" [DONE]")

		fmt.Print("AMBIL LPP NONAIR LOKET PER KODE")

		ctx_knonl := context.Background()
		q_knonl := fmt.Sprintf("SELECT @id:=@id+1,urutan AS kode,jenis,DATE(waktubayar) AS tglbayar,total,'Data berikut belum masuk ke billing' AS keterangan FROM nonair, (SELECT @id:=0) AS id WHERE DATE(waktubayar) IN ('%s') AND flaglunas=1 AND flagbatal=0 AND flaghapus=0 AND (COALESCE(noangsuran,'')='' OR COALESCE(noangsuran,'')='-') AND flagangsur=0 AND jenis NOT IN ('JNS-36') ORDER BY waktubayar,kode ASC", strings.Join(tglNonSelisih, "','"))
		lpp_nonkode, e_lppnonkode := dbloket.QueryContext(ctx_knonl, q_knonl)
		if e_lppnonkode != nil {
			log.Fatalf("Gagal ambil data : %v", e_lppnonkode)
		}
		defer lpp_nonkode.Close()

		var dataNonL []DataNonL
		for lpp_nonkode.Next() {
			var KL DataNonL
			elpp := lpp_nonkode.Scan(&KL.Id, &KL.Kode, &KL.Jenis, &KL.TglBayar, &KL.Total, &KL.Keterangan)
			if elpp != nil {
				log.Fatal("Data kosong :", elpp)
			}
			dataNonL = append(dataNonL, KL)
		}
		defer lpp_nonkode.Close()

		fmt.Println(" [DONE]")

		fmt.Print("AMBIL LPP NONAIR BILLING PER KODE")

		ctx_knonb := context.Background()
		q_knonb := fmt.Sprintf("SELECT urutan AS kode,jenis,DATE(waktubayar) AS tglbayar,total FROM nonair WHERE DATE(waktubayar) IN ('%s') AND flaglunas=1 AND flagbatal=0 AND flaghapus=0 AND (COALESCE(noangsuran,'')='' OR COALESCE(noangsuran,'')='-') AND flagangsur=0 AND jenis NOT IN ('JNS-36') ORDER BY waktubayar,kode ASC", strings.Join(tglNonSelisih, "','"))
		lpp_nonkode2, e_lppnonkode2 := dbbilling.QueryContext(ctx_knonb, q_knonb)
		if e_lppnonkode2 != nil {
			log.Fatalf("Gagal ambil data : %v", e_lppnonkode2)
		}
		defer lpp_nonkode2.Close()

		var dataNonB []DataNonB
		for lpp_nonkode2.Next() {
			var KB DataNonB
			elpp := lpp_nonkode2.Scan(&KB.Kode, &KB.Jenis, &KB.TglBayar, &KB.Total)
			if elpp != nil {
				log.Fatal("Data kosong :", elpp)
			}
			dataNonB = append(dataNonB, KB)
		}
		defer lpp_nonkode2.Close()

		fmt.Println(" [DONE]")

		fmt.Print("BANDINGKAN LPP NONAIR LOKET DAN BILLING PER KODE")

		// BANDINGKAN LPP LOKET DAN BILLING
		for _, kodeNonBill := range dataNonB {
			for i, kodeNonLoket := range dataNonL {
				if kodeNonBill.Kode == kodeNonLoket.Kode {
					if kodeNonBill.Total == kodeNonLoket.Total {
						dataNonL[i].Keterangan = ""
					} else if kodeNonBill.Total != kodeNonLoket.Total {
						dataNonL[i].Keterangan = fmt.Sprintf("Selisih total untuk tagihan nonair ini => Billing : %.2f Loket : %.2f", kodeNonBill.Total, kodeNonLoket.Total)
					}
				}
			}
		}

		fmt.Println(" [DONE]")

		fmt.Print("INSERT PESAN SELISIH LPP NONAIR LOKET - BILLING")
		var resultNon, pesanNon string
		pesan = pesan + "\n4. LPP . Nonair \n\n"
		for _, dtglnonl := range tglnonl {
			if dtglnonl.Keterangan != "" {
				pesan = pesan + fmt.Sprintf("%s,KODE,Jenis Nonair,%s\n", dtglnonl.TglBayar, dtglnonl.Keterangan)
			}
			for _, dataNon := range dataNonL {
				if dtglnonl.TglBayar == dataNon.TglBayar {
					if dataNon.Keterangan != "" {
						resultNon = fmt.Sprintf("%s,%s,%s,%s\n", dataNon.TglBayar, dataNon.Kode, dataNon.Jenis, dataNon.Keterangan)
						pesan = pesan + fmt.Sprintf("%s,%s,%s,%s\n", dataNon.TglBayar, dataNon.Kode, dataNon.Jenis, dataNon.Keterangan)
					}
				}
			}
		}

		if resultNon == "" {
			pesan = pesan + "TIDAK ADA SELISIH PENERIMAAN NONAIR\n"
			pesanNon = " => TIDAK ADA SELISIH PENERIMAAN NONAIR"
		} else {
			pesan = pesan + resultNon
			pesanNon = " => TERDAPAT SELISIH PENERIMAAN NONAIR"
		}
		fmt.Println(pesanNon + " [DONE]")
	}
	// UNTUK TRACE ERROR
	// for _, cetakAngs1 := range dataAngsNonL {
	// if cetakAngs.Keterangan != "" {
	// fmt.Println(cetakAngs1)
	// }
	// }

	// fmt.Print(pesan)

	return pesan
}

func CekPembatalan(DBUser, DBPass, DBName, DBHost, DBPort, IdPDAM, pesan string) string {
	pesan = pesan + "\nCEK PEMBATALAN LPP LOKET - BILLING \n"

	tahun_ini := time.Now().Year()
	tahun_lalu := time.Now().Year() //- 1
	bulan := time.Now()
	// bulan1 := bulan.Format("01")
	// periode := fmt.Sprintf("%d%s", tahun_ini, bulan1)
	sekarang := bulan.Format("2006-01-02")

	// Ganti nilai-nilai berikut sesuai dengan informasi koneksi MySQL Anda
	dbUser := DBUser
	dbPass := DBPass
	dbName := DBName
	dbHost := DBHost
	dbPort := DBPort
	idPDAM := IdPDAM

	for tahun := tahun_lalu; tahun <= tahun_ini; tahun++ {
		fmt.Println("\n- CEK PEMBATALAN LPP", tahun)
		for {
			db := connmaster.ConnMaster(dbUser, dbPass, dbName, dbHost, dbPort)
			defer db.Close()

			// Mengecek koneksi ke database
			err := db.Ping()
			if err != nil {
				fmt.Println("Tidak dapat terhubung ke database:", err)
				// pesan = "Tidak dapat terhubung ke database:" + err.Error()
				// telebot.TeleBot(pesan)
				goto Sleep
			} else {

				break

			}
		Sleep:
			time.Sleep(5 * time.Second)
		}

		// START CEK PELANGGAN BILLING
		db := connmaster.ConnMaster(dbUser, dbPass, dbName, dbHost, dbPort)
		defer db.Close()

		ctx := context.Background()
		//var db *sql.DB
		script := "SELECT idpdam,nama_pdam,`database` FROM sipintar_pdam_config WHERE idpdam = ? "
		rows, err := db.QueryContext(ctx, script, idPDAM)
		if err != nil {
			log.Fatal("Gagal ambil data : ", err)
			//goto Sleep2
		}

		//var dataPelLoket []DataPelLoket
		var Ip_bill, User_bill, Pass_bill, DB_bill, Port_bill string
		var Ip_loket, User_loket, Pass_loket, DB_loket, Port_loket string

		for rows.Next() {
			var idpdam, nama_pdam, database string
			err := rows.Scan(&idpdam, &nama_pdam, &database)
			if err != nil {
				log.Fatalf("Data kosong : %v", err)
			}

			//Menampung Array hasil select config database
			databs, err := readJSONArray(database)
			if err != nil {
				log.Fatal("Gagal membaca data :", err)
			}

			for _, data := range databs {

				if data.Tipe == "billing" {
					Ip_bill = data.DBHost
					User_bill = data.DBUser
					Pass_bill = data.DBPass
					DB_bill = data.DBName
					Port_bill = data.DBPort

				}
				if data.Tipe == "loket" {
					Ip_loket = data.DBHost
					User_loket = data.DBUser
					Pass_loket = data.DBPass
					DB_loket = data.DBName
					Port_loket = data.DBPort

				}

			}
		}

		//  CEK KONEKSI LOKET
		dbbilling := connmaster.ConnBilling(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)
		defer dbbilling.Close()

		// Mengecek koneksi ke database Loket
		err = dbbilling.Ping()
		if err != nil {
			log.Fatalf("Tidak dapat terhubung ke database: %v", err)
		}

		for {
			dbbilling := connmaster.ConnBilling(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)
			defer dbbilling.Close()

			// Mengecek koneksi ke database
			err := dbbilling.Ping()
			if err != nil {
				fmt.Println("Tidak dapat terhubung ke database:", err)
				// pesan = "Tidak dapat terhubung ke database:" + err.Error()
				// telebot.TeleBot(pesan)
				goto SleepB1
			} else {

				break

			}
		SleepB1:
			time.Sleep(5 * time.Second)
		}

		//  CEK KONEKSI LOKET
		dbloket := connmaster.ConnLoket(User_loket, Pass_loket, DB_loket, Ip_loket, Port_loket)
		defer dbloket.Close()

		// Mengecek koneksi ke database Loket
		err = dbloket.Ping()
		if err != nil {
			log.Fatalf("Tidak dapat terhubung ke database: %v", err)
		}

		fmt.Print("CEK TABEL BAYAR TAHUN BILLING")
		ctx_cektabel := context.Background()
		qlpp_cektabel := fmt.Sprint("SHOW TABLES LIKE 'bayar", tahun, "%'")
		cek_tabel, e_cektabel := dbbilling.QueryContext(ctx_cektabel, qlpp_cektabel)
		if e_cektabel != nil {
			log.Fatalf("Gagal ambil data : %v", e_cektabel)
		}

		defer cek_tabel.Close()

		var tabel_ada string
		if cek_tabel.Next() {
			tabel_ada = "1"
		} else {
			tabel_ada = "0"
		}

		fmt.Println(" -> TABEL bayar", tahun, " TERSEDIA [DONE]")

		pesan = pesan + "\nPEMBATALAN TAHUN " + strconv.Itoa(tahun) + "\n"
		fmt.Println("1. CEK PEMBATALAN LPP AIR")
		// START CEK PEMBATALAN LPP AIR
		// dbloket := CekConnLoket(Ip_loket, User_loket, Pass_loket, DB_loket, Port_loket)

		if tabel_ada == "1" {

			fmt.Print("AMBIL DATA LPP BILLING")
			ctx_lppbilling := context.Background()
			qair_billing := fmt.Sprint("SELECT kode,rekair FROM bayar", tahun, " WHERE YEAR(tglbayar)=? AND flag<>4 AND flaglunas=1 and flagbatal=0 AND flagangsur=0 AND (COALESCE(noangsuran,'')='' OR COALESCE(noangsuran,'')='-') AND DATE(tglbayar)<?")
			lpp_bill, e_lppbilling := dbbilling.QueryContext(ctx_lppbilling, qair_billing, tahun, sekarang)
			if e_lppbilling != nil {
				log.Fatalf("Gagal ambil data : %v", e_lppbilling)
			}

			defer lpp_bill.Close()

			// TAMPUNG KODE LPP BILLING
			var kodepairb []KodePAirB
			for lpp_bill.Next() {
				var kodeairB KodePAirB
				e_lppbill := lpp_bill.Scan(&kodeairB.Kode, &kodeairB.Rekair)
				if e_lppbill != nil {
					log.Fatal("Data kosong :", e_lppbill)
				}
				kodepairb = append(kodepairb, kodeairB)

			}

			fmt.Println(" [DONE]")
			defer lpp_bill.Close()

			fmt.Print("TAMPUNG KODE LPP AIR BILLING")
			var kodeAirBill []string
			for _, data2 := range kodepairb {

				kodeAirBill = append(kodeAirBill, data2.Kode)

			}
			fmt.Println(" [DONE]")

			fmt.Print("CEK LPP BILLING KE PIUTANG LOKET")
			ctx_airloket := context.Background()
			qair_loket := fmt.Sprintf("SELECT @id:=@id+1 AS id,kode,DATE(tglbayar) AS tglbayar,rekair,CONCAT('Rekening dibatalkan oleh Kasir : ',kasir,' pada tanggal : ',tglbatal,' dengan alasan : ',alasanbatal) AS keterangan FROM piutang,(SELECT @id:=0) AS id WHERE kode IN ('%s') AND flag<>4 AND flaglunas=0 and flagbatal=1 AND flagangsur=0 AND (COALESCE(noangsuran,'')='' OR COALESCE(noangsuran,'')='-') ORDER BY tglbayar,kode ASC", strings.Join(kodeAirBill, "','"))
			lpp_kode, e_lppkode := dbloket.QueryContext(ctx_airloket, qair_loket)
			if e_lppkode != nil {
				log.Fatalf("Gagal ambil data : %v", e_lppkode)
			}

			defer lpp_kode.Close()

			var kodepairl []KodePAirL
			for lpp_kode.Next() {
				var kodeairL KodePAirL
				e_lppkode := lpp_kode.Scan(&kodeairL.Id, &kodeairL.Kode, &kodeairL.Rekair, &kodeairL.Keterangan)
				if e_lppkode != nil {
					log.Fatal("Data kosong :", e_lppkode)
				}
				kodepairl = append(kodepairl, kodeairL)

			}

			fmt.Println(" [DONE]")
			defer lpp_kode.Close()

			fmt.Print("INSERT PESAN PEMBATALAN TAGIHAN AIR")
			var resultAir, pesanAir string
			pesan = pesan + "\n1. Pembatalan Air \n\n"

			for _, dataPemAir := range kodepairl {
				if dataPemAir.Keterangan != "" {
					resultAir = fmt.Sprintf("%s,%f,%s\n", dataPemAir.Kode, dataPemAir.Rekair, dataPemAir.Keterangan)
					pesan = pesan + fmt.Sprintf("%s,%f,%s\n", dataPemAir.Kode, dataPemAir.Rekair, dataPemAir.Keterangan)
				}
			}

			if resultAir == "" {
				pesan = pesan + "TIDAK ADA PEMBATALAN TAGIHAN AIR TERSANGKUT\n"
				pesanAir = " => TIDAK ADA PEMBATALAN TAGIHAN AIR TERSANGKUT"
			} else {
				pesan = pesan + resultAir
				pesanAir = " => TERDAPAT PEMBATALAN TAGIHAN AIR TERSANGKUT"
			}
			fmt.Println(pesanAir + " [DONE]")

		} else {
			pesan = pesan + fmt.Sprintf("TABEL bayar %d TIDAK DITEMUKAN", tahun)
		}

		fmt.Println("\n2. CEK PEMBATALAN LPP ANGS. AIR")
		// START CEK PEMBATALAN LPP AIR
		// dbloket := CekConnLoket(Ip_loket, User_loket, Pass_loket, DB_loket, Port_loket)

		if tabel_ada == "1" {

			fmt.Print("AMBIL DATA LPP ANGS. AIR BILLING")
			ctx_lppbilling := context.Background()
			qair_billing := fmt.Sprint("SELECT kode,rekair FROM bayar", tahun, " WHERE YEAR(tglbayar)=? AND flag<>4 AND flaglunas=1 and flagbatal=0 AND flagangsur=0 AND (COALESCE(noangsuran,'')='' OR COALESCE(noangsuran,'')='-') AND DATE(tglbayar)<?")
			lpp_bill, e_lppbilling := dbbilling.QueryContext(ctx_lppbilling, qair_billing, tahun, sekarang)
			if e_lppbilling != nil {
				log.Fatalf("Gagal ambil data : %v", e_lppbilling)
			}

			defer lpp_bill.Close()

			// TAMPUNG KODE LPP BILLING
			var kodepangairb []KodePAngAirB
			for lpp_bill.Next() {
				var kodeangairB KodePAngAirB
				e_lppbill := lpp_bill.Scan(&kodeangairB.Kode, &kodeangairB.Rekair)
				if e_lppbill != nil {
					log.Fatal("Data kosong :", e_lppbill)
				}
				kodepangairb = append(kodepangairb, kodeangairB)

			}

			fmt.Println(" [DONE]")
			defer lpp_bill.Close()

			fmt.Print("TAMPUNG KODE LPP ANGS. AIR BILLING")
			var kodeAngAirBill []string
			for _, data2 := range kodepangairb {

				kodeAngAirBill = append(kodeAngAirBill, data2.Kode)

			}
			fmt.Println(" [DONE]")

			fmt.Print("CEK LPP ANGS. AIR BILLING KE ANGS. AIR LOKET")
			ctx_airloket := context.Background()
			qair_loket := fmt.Sprintf("SELECT @id:=@id+1 AS id,urutan AS kode,DATE(waktubayar) AS tglbayar,total-dendatunggakan AS rekair,CONCAT('Rekening dibatalkan oleh Kasir : ',kasir,' pada tanggal : ',waktubatal,' dengan alasan : ',alasanbatal) AS keterangan FROM nonair,(SELECT @id:=0) AS id WHERE urutan IN ('%s') AND flag<>4 AND flaglunas=0 and flagbatal=1 AND flagangsur=0 AND (COALESCE(noangsuran,'')='' OR COALESCE(noangsuran,'')='-') ORDER BY waktubayar,kode ASC", strings.Join(kodeAngAirBill, "','"))
			lpp_kode, e_lppkode := dbloket.QueryContext(ctx_airloket, qair_loket)
			if e_lppkode != nil {
				log.Fatalf("Gagal ambil data : %v", e_lppkode)
			}

			defer lpp_kode.Close()

			var kodepangairl []KodePAngAirL
			for lpp_kode.Next() {
				var kodeangairL KodePAngAirL
				e_lppkode := lpp_kode.Scan(&kodeangairL.Id, &kodeangairL.Kode, &kodeangairL.Rekair, &kodeangairL.Keterangan)
				if e_lppkode != nil {
					log.Fatal("Data kosong :", e_lppkode)
				}
				kodepangairl = append(kodepangairl, kodeangairL)

			}

			fmt.Println(" [DONE]")
			defer lpp_kode.Close()

			fmt.Print("INSERT PESAN PEMBATALAN TAGIHAN ANGS. AIR")
			var resultAngAir, pesanAngAir string
			pesan = pesan + "\n2. Pembatalan Angs. Air \n\n"

			for _, dataPemAngAir := range kodepangairl {
				if dataPemAngAir.Keterangan != "" {
					resultAngAir = fmt.Sprintf("%s,%f,%s\n", dataPemAngAir.Kode, dataPemAngAir.Rekair, dataPemAngAir.Keterangan)
					pesan = pesan + fmt.Sprintf("%s,%f,%s\n", dataPemAngAir.Kode, dataPemAngAir.Rekair, dataPemAngAir.Keterangan)
				}
			}

			if resultAngAir == "" {
				pesan = pesan + "TIDAK ADA PEMBATALAN TAGIHAN ANGS. AIR TERSANGKUT\n"
				pesanAngAir = " => TIDAK ADA PEMBATALAN TAGIHAN ANGS. AIR TERSANGKUT"
			} else {
				pesan = pesan + resultAngAir
				pesanAngAir = " => TERDAPAT PEMBATALAN TAGIHAN ANGS. AIR TERSANGKUT"
			}
			fmt.Println(pesanAngAir + " [DONE]")

		} else {
			pesan = pesan + fmt.Sprintf("TABEL bayar %d TIDAK DITEMUKAN", tahun)
		}

		fmt.Println("\n3. CEK PEMBATALAN LPP ANGS. NONAIR")
		fmt.Print("AMBIL LPP ANGS NONAIR BILLING")

		ctx_lppbilling := context.Background()
		lpp_bill, e_lppbilling := dbbilling.QueryContext(ctx_lppbilling, "SELECT kode,total FROM nonair WHERE YEAR(waktubayar)=? AND flaglunas=1 AND flagbatal=0 AND flaghapus=0 AND COALESCE(noangsuran,'')<>'' AND flagangsur=1 AND jenis NOT IN ('JNS-36','JNS-37') AND DATE(waktubayar)<?", tahun, sekarang)
		if e_lppbilling != nil {
			log.Fatalf("Gagal ambil data : %v", e_lppbilling)
		}

		defer lpp_bill.Close()

		// TAMPUNG KODE LPP BILLING
		var kodepangnonairb []KodePAngNonAirB
		for lpp_bill.Next() {
			var kodeangnonairB KodePAngNonAirB
			e_lppbill := lpp_bill.Scan(&kodeangnonairB.Kode, &kodeangnonairB.Total)
			if e_lppbill != nil {
				log.Fatal("Data kosong :", e_lppbill)
			}
			kodepangnonairb = append(kodepangnonairb, kodeangnonairB)

		}

		fmt.Println(" [DONE]")
		defer lpp_bill.Close()

		fmt.Print("TAMPUNG KODE LPP ANGS. NONAIR BILLING")
		var kodeAngNonAirBill []string
		for _, data2 := range kodepangnonairb {

			kodeAngNonAirBill = append(kodeAngNonAirBill, data2.Kode)

		}
		fmt.Println(" [DONE]")

		fmt.Print("CEK LPP ANGS. NONAIR BILLING KE ANGS. NONAIR LOKET")
		ctx_airloket := context.Background()
		qair_loket := fmt.Sprintf("SELECT @id:=@id+1 AS id,urutan AS kode,DATE(waktubayar) AS tglbayar,total,CONCAT('Tagihan dibatalkan oleh Kasir : ',kasir,' pada tanggal : ',waktubatal,' dengan alasan : ',alasanbatal) AS keterangan FROM nonair,(SELECT @id:=0) AS id WHERE urutan IN ('%s') AND flaglunas=0 AND flagbatal=1 AND flaghapus=0 AND (COALESCE(noangsuran,'')='' OR COALESCE(noangsuran,'')='-') AND flagangsur=1 AND jenis NOT IN ('JNS-36','JNS-37') ORDER BY waktubayar,kode ASC", strings.Join(kodeAngNonAirBill, "','"))
		lpp_kode, e_lppkode := dbloket.QueryContext(ctx_airloket, qair_loket)
		if e_lppkode != nil {
			log.Fatalf("Gagal ambil data : %v", e_lppkode)
		}

		defer lpp_kode.Close()

		var kodepangnonairl []KodePAngNonAirL
		for lpp_kode.Next() {
			var kodeangnonairL KodePAngNonAirL
			e_lppkode := lpp_kode.Scan(&kodeangnonairL.Id, &kodeangnonairL.Kode, &kodeangnonairL.Total, &kodeangnonairL.Keterangan)
			if e_lppkode != nil {
				log.Fatal("Data kosong :", e_lppkode)
			}
			kodepangnonairl = append(kodepangnonairl, kodeangnonairL)

		}

		fmt.Println(" [DONE]")

		fmt.Print("INSERT PESAN PEMBATALAN TAGIHAN ANGS. NONAIR")
		var resultAngNonAir, pesanAngNonAir string
		pesan = pesan + "\n3. Pembatalan Angs. Nonair \n\n"

		for _, dataPemAngNonAir := range kodepangnonairl {
			if dataPemAngNonAir.Keterangan != "" {
				resultAngNonAir = fmt.Sprintf("%s,%f,%s\n", dataPemAngNonAir.Kode, dataPemAngNonAir.Total, dataPemAngNonAir.Keterangan)
				pesan = pesan + fmt.Sprintf("%s,%f,%s\n", dataPemAngNonAir.Kode, dataPemAngNonAir.Total, dataPemAngNonAir.Keterangan)
			}
		}

		if resultAngNonAir == "" {
			pesan = pesan + "TIDAK ADA PEMBATALAN TAGIHAN ANGS. NONAIR TERSANGKUT\n"
			pesanAngNonAir = " => TIDAK ADA PEMBATALAN TAGIHAN ANGS. NONAIR TERSANGKUT"
		} else {
			pesan = pesan + resultAngNonAir
			pesanAngNonAir = " => TERDAPAT PEMBATALAN TAGIHAN ANGS. NONAIR TERSANGKUT"
		}
		fmt.Println(pesanAngNonAir + " [DONE]")

		fmt.Println("\n4. CEK PEMBATALAN LPP NONAIR")
		fmt.Print("AMBIL LPP NONAIR BILLING")

		ctx_nonbilling := context.Background()
		lpp_nonbill, e_nonlppbilling := dbbilling.QueryContext(ctx_nonbilling, "SELECT kode,total FROM nonair WHERE YEAR(waktubayar)=? AND flaglunas=1 AND flagbatal=0 AND flaghapus=0 AND (COALESCE(noangsuran,'')='' OR COALESCE(noangsuran,'')='-') AND flagangsur=0 AND jenis NOT IN ('JNS-36') AND DATE(waktubayar)<?", tahun, sekarang)
		if e_nonlppbilling != nil {
			log.Fatalf("Gagal ambil data : %v", lpp_nonbill)
		}

		defer lpp_nonbill.Close()

		// TAMPUNG KODE LPP BILLING
		var kodepnonairb []KodePNonAirB
		for lpp_nonbill.Next() {
			var kodenonairB KodePNonAirB
			e_lppbill := lpp_nonbill.Scan(&kodenonairB.Kode, &kodenonairB.Total)
			if e_lppbill != nil {
				log.Fatal("Data kosong :", e_lppbill)
			}
			kodepnonairb = append(kodepnonairb, kodenonairB)

		}

		fmt.Println(" [DONE]")
		defer lpp_nonbill.Close()

		fmt.Print("TAMPUNG KODE LPP NONAIR BILLING")
		var kodeNonAirBill []string
		for _, data2 := range kodepnonairb {

			kodeNonAirBill = append(kodeNonAirBill, data2.Kode)

		}
		fmt.Println(" [DONE]")

		fmt.Print("CEK LPP NONAIR BILLING KE NONAIR LOKET")
		ctx_nonairloket := context.Background()
		qnonair_loket := fmt.Sprintf("SELECT @id:=@id+1 AS id,urutan AS kode,DATE(waktubayar) AS tglbayar,total,CONCAT('Tagihan dibatalkan oleh Kasir : ',kasir,' pada tanggal : ',waktubatal,' dengan alasan : ',alasanbatal) AS keterangan FROM nonair,(SELECT @id:=0) AS id WHERE urutan IN ('%s') AND flaglunas=0 AND flagbatal=1 AND flaghapus=0 AND (COALESCE(noangsuran,'')='' OR COALESCE(noangsuran,'')='-') AND flagangsur=1 AND jenis NOT IN ('JNS-36','JNS-37') ORDER BY waktubayar,kode ASC", strings.Join(kodeAngNonAirBill, "','"))
		lpp_nonkode, e_lppnonkode := dbloket.QueryContext(ctx_nonairloket, qnonair_loket)
		if e_lppnonkode != nil {
			log.Fatalf("Gagal ambil data : %v", e_lppnonkode)
		}

		defer lpp_nonkode.Close()

		var kodepnonairl []KodePNonAirL
		for lpp_kode.Next() {
			var kodenonairL KodePNonAirL
			e_lppkode := lpp_kode.Scan(&kodenonairL.Id, &kodenonairL.Kode, &kodenonairL.Total, &kodenonairL.Keterangan)
			if e_lppkode != nil {
				log.Fatal("Data kosong :", e_lppkode)
			}
			kodepnonairl = append(kodepnonairl, kodenonairL)

		}

		fmt.Println(" [DONE]")

		fmt.Print("INSERT PESAN PEMBATALAN TAGIHAN NONAIR")
		var resultNonAir, pesanNonAir string
		pesan = pesan + "\n4. Pembatalan Nonair \n\n"

		for _, dataPemNonAir := range kodepnonairl {
			if dataPemNonAir.Keterangan != "" {
				resultNonAir = fmt.Sprintf("%s,%f,%s\n", dataPemNonAir.Kode, dataPemNonAir.Total, dataPemNonAir.Keterangan)
				pesan = pesan + fmt.Sprintf("%s,%f,%s\n", dataPemNonAir.Kode, dataPemNonAir.Total, dataPemNonAir.Keterangan)
			}
		}

		if resultNonAir == "" {
			pesan = pesan + "TIDAK ADA PEMBATALAN TAGIHAN NONAIR TERSANGKUT\n"
			pesanNonAir = " => TIDAK ADA PEMBATALAN TAGIHAN NONAIR TERSANGKUT"
		} else {
			pesan = pesan + resultNonAir
			pesanNonAir = " => TERDAPAT PEMBATALAN TAGIHAN NONAIR TERSANGKUT"
		}
		fmt.Println(pesanNonAir + " [DONE]")
	}

	return pesan
}

func CekPiutang(DBUser, DBPass, DBName, DBHost, DBPort, IdPDAM, pesan string) string {
	pesan = pesan + "\nCEK SELISIH PIUTANG \n"

	tahun_ini := time.Now().Year()
	tahun_lalu := time.Now().Year()

	// fmt.Println(tahun_ini)

	dbUser := DBUser
	dbPass := DBPass
	dbName := DBName
	dbHost := DBHost
	dbPort := DBPort
	idPDAM := IdPDAM

	for tahun := tahun_lalu; tahun <= tahun_ini; tahun++ {
		fmt.Println("\n- CEK SELISIH PIUTANG", tahun)
		for {
			db := connmaster.ConnMaster(dbUser, dbPass, dbName, dbHost, dbPort)
			defer db.Close()

			// Mengecek koneksi ke database
			err := db.Ping()
			if err != nil {
				fmt.Println("Tidak dapat terhubung ke database:", err)
				// pesan = "Tidak dapat terhubung ke database:" + err.Error()
				// telebot.TeleBot(pesan)
				goto Sleep
			} else {

				break

			}
		Sleep:
			time.Sleep(5 * time.Second)
		}

		db := connmaster.ConnMaster(dbUser, dbPass, dbName, dbHost, dbPort)
		defer db.Close()

		ctx := context.Background()
		script := "SELECT idpdam,nama_pdam,`database` FROM sipintar_pdam_config WHERE idpdam = ? "
		rows, err := db.QueryContext(ctx, script, idPDAM)
		if err != nil {
			log.Fatal("Gagal ambil data : ", err)
			//goto Sleep2
		}

		// CONF BILL
		var Ip_bill, User_bill, Pass_bill, DB_bill, Port_bill string
		// CONF LOKET
		var Ip_loket, User_loket, Pass_loket, DB_loket, Port_loket string

		for rows.Next() {
			var idpdam, nama_pdam, database string
			err := rows.Scan(&idpdam, &nama_pdam, &database)
			if err != nil {
				log.Fatalf("Data kosong : %v", err)
			}

			//Menampung Array hasil select config database
			databs, err := readJSONArray(database)
			if err != nil {
				log.Fatal("Gagal membaca data :", err)
			}

			for _, data := range databs {
				// CONF BILL
				if data.Tipe == "billing" {
					Ip_bill = data.DBHost
					User_bill = data.DBUser
					Pass_bill = data.DBPass
					DB_bill = data.DBName
					Port_bill = data.DBPort

				}
				// CONF LOKET
				if data.Tipe == "loket" {
					Ip_loket = data.DBHost
					User_loket = data.DBUser
					Pass_loket = data.DBPass
					DB_loket = data.DBName
					Port_loket = data.DBPort

				}

			}
		}

		dbbilling := connmaster.ConnBilling(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)
		defer dbbilling.Close()

		err = dbbilling.Ping()
		if err != nil {
			log.Fatalf("Tidak dapat terhubung ke database: %v", err)
		}

		for {
			dbbilling := connmaster.ConnBilling(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)
			defer dbbilling.Close()

			// Mengecek koneksi ke database
			err := dbbilling.Ping()
			if err != nil {
				fmt.Println("Tidak dapat terhubung ke database:", err)
				// pesan = "Tidak dapat terhubung ke database:" + err.Error()
				// telebot.TeleBot(pesan)
				goto SleepB1
			} else {

				break

			}
		SleepB1:
			time.Sleep(5 * time.Second)
		}

		dbloket := connmaster.ConnLoket(User_loket, Pass_loket, DB_loket, Ip_loket, Port_loket)
		defer dbloket.Close()

		err = dbloket.Ping()
		if err != nil {
			log.Fatalf("Tidak dapat terhubung ke database: %v", err)
		}

		for {
			dbloket := connmaster.ConnLoket(User_loket, Pass_loket, DB_loket, Ip_loket, Port_loket)
			defer dbloket.Close()

			// Mengecek koneksi ke database
			err := dbloket.Ping()
			if err != nil {
				fmt.Println("Tidak dapat terhubung ke database:", err)
				// pesan = "Tidak dapat terhubung ke database:" + err.Error()
				// telebot.TeleBot(pesan)
				goto SleepL1
			} else {

				break

			}
		SleepL1:
			time.Sleep(5 * time.Second)
		}

		fmt.Print("CEK TABEL BAYAR TAHUN BILLING")
		// ctx = context.Background()
		qlpp_cektabel := fmt.Sprint("SHOW TABLES LIKE 'bayar", tahun, "%'")
		rows, err = dbbilling.QueryContext(ctx, qlpp_cektabel)
		if err != nil {
			log.Fatalf("Gagal ambil data : %v", err)
		}

		defer rows.Close()

		var tabel_ada string
		if rows.Next() {
			tabel_ada = "1"
		} else {
			tabel_ada = "0"
		}

		if tabel_ada == "1" {
			fmt.Println(" -> TABEL BAYAR", tahun, "TERSEDIA [DONE]")

			fmt.Print("CEK PERIODE SUDAH BERJALAN")
			// ctx_cekper := context.Background()
			qcek_per := fmt.Sprint("SELECT DATE_FORMAT(tglbayar,'%Y%m') AS periode,CONCAT(b.nama,' ','", tahun, "') AS bulan,LAST_DAY(tglbayar - INTERVAL 1 MONTH) AS tgllalu,IF(DATE(MAX(tglbayar))=LAST_DAY(MAX(tglbayar)),LAST_DAY(MAX(tglbayar)),DATE(MAX(tglbayar))) AS tglkini,DATE_FORMAT(CONCAT(YEAR(a.tglbayar),'-',MONTH(a.tglbayar),'-01') - INTERVAL 1 MONTH,'%Y%m') AS periode2 FROM bayar", tahun, " a LEFT JOIN namabulan b ON MONTH(a.tglbayar)=b.bulan GROUP BY DATE_FORMAT(tglbayar,'%Y%m') ORDER BY tglbayar ASC;")
			rows, err = dbbilling.QueryContext(ctx, qcek_per)

			if err != nil {
				log.Fatalf("Gagal ambil data : %v", err)
			}

			defer rows.Close()

			var periode []Periode
			for rows.Next() {
				var per Periode
				err = rows.Scan(&per.Kode, &per.bulan, &per.Tgllalu, &per.Tglkini, &per.Kode2)
				if err != nil {
					log.Fatal("Data kosong :", err)
				}
				periode = append(periode, per)
			}
			fmt.Printf(" [DONE]\n")

			fmt.Print("MENGOSONGKAN TABEL TAMPUNGAN")
			ctx_kosongdump := context.Background()
			cek_kosongdump, e_cekkosdump := dbbilling.QueryContext(ctx_kosongdump, "DELETE FROM dumplaporanpiutang WHERE user='SERVICE'")

			if e_cekkosdump != nil {
				log.Fatalf("Gagal mengosongkan dumplaporanpiutang : %v", e_cekkosdump)
			}

			defer cek_kosongdump.Close()

			// ctx_kosongdump2 := context.Background()
			rows, err = dbbilling.QueryContext(ctx, "CREATE TABLE IF NOT EXISTS dumplaporanpiutang_manual LIKE dumplaporanpiutang")

			if err != nil {
				log.Fatalf("Gagal mengosongkan dumplaporanpiutang : %v", err)
			}

			defer rows.Close()

			fmt.Printf(" [DONE]\n")

			for _, data := range periode {
				fmt.Println("")
				fmt.Println("-> CEK PIUTANG BULAN", strings.ToUpper(data.bulan))

				ctx_kosongdump3 := context.Background()
				cek_kosongdump3, e_cekkosdump3 := dbbilling.QueryContext(ctx_kosongdump3, "DELETE FROM dumplaporanpiutang_manual WHERE user='SERVICE'")

				if e_cekkosdump3 != nil {
					log.Fatalf("Gagal mengosongkan dumplaporanpiutang : %v", e_cekkosdump3)
				}

				defer cek_kosongdump3.Close()

				// var piutang []Piutang
				// //var posting []Posting
				// var koreksi []Koreksi

				fmt.Println("- PROSES PIUTANG MANUAL ", strings.ToUpper(data.bulan))

				// // AMBIL TAHUN
				// ctx_tahun := context.Background()
				qtahuns := fmt.Sprintf("SELECT tahun_per FROM (SELECT LEFT(periode,4) AS tahun_per FROM piutang WHERE flaglunas='0' AND flagangsur='0' GROUP BY LEFT(periode,4) UNION SELECT LEFT(periode,4) AS tahun_per FROM bayar%d WHERE flaglunas='0' AND flagangsur='0' GROUP BY LEFT(periode,4)) a GROUP BY tahun_per", tahun)
				rows1, err1 := dbbilling.QueryContext(ctx, qtahuns)
				if err1 != nil {
					log.Fatalf("Gagal ambil data : %v", err1)
				}
				defer rows1.Close()

				var tahuns string
				for rows1.Next() {
					err1 = rows1.Scan(&tahuns)
					if err1 != nil {
						log.Fatal("Data kosong : ", err1)
					}

					Callback_ConBill(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)

					fmt.Printf("AMBIL DATA PIUTANG MANUAL TAHUN : %s       \r", tahuns)

					var q_piutang string
					// ctx_cek := context.Background()
					q_cek := fmt.Sprintf("SELECT * FROM piutang WHERE flaglunas='0' AND flagangsur='0' AND periode<'%s' AND LEFT(periode,4)='%s'", data.Kode, tahuns)
					rows, err := dbbilling.QueryContext(ctx, q_cek)
					if err != nil {
						log.Fatalf("Gagal ambil data : %v", err)
					}

					defer rows.Close()

					var cekisi string
					if rows.Next() {
						cekisi = "1"
					} else {
						cekisi = "0"
					}

					if cekisi == "1" {
						// ctx_piutang := context.Background()
						q_piutang = fmt.Sprintf("REPLACE INTO dumplaporanpiutang_manual SELECT CONCAT('SERVICE.',c.kode),'SERVICE', c.kode,c.periode,c.nosamb,c.bulan,c.kodegol,c.kodediameter,c.koderayon,c.stanlalu,c.stanskrg,c.pakai,c.prog1,c.prog2,c.prog3,c.prog4,c.biayapemakaian,c.administrasi,c.pemeliharaan,c.retribusi,c.meterai,c.rekair,0 AS denda,c.administrasilain,c.pemeliharaanlain,c.retribusilain,c.pelayanan,c.rekair+c.administrasilain+c.pemeliharaanlain+c.retribusilain+c.pelayanan  AS total,pakai2 FROM piutang  c WHERE c.flaglunas='0' AND c.flagangsur='0' AND c.periode<'%s' AND LEFT(c.periode,4)='%s'", data.Kode, tahuns)
						rows, err := dbbilling.QueryContext(ctx, q_piutang)
						if err != nil {
							log.Fatalf("Gagal ambil data : %v", err)
						}

						defer rows.Close()
					}

					fmt.Printf("AMBIL DATA PIUTANG MANUAL TAHUN : %s [DONE]\r", tahuns)
					time.Sleep(500 * time.Millisecond)
				}
				fmt.Println("AMBIL DATA PIUTANG MANUAL [DONE]              ")

				// ctx_tahun = context.Background()
				qtahuns = fmt.Sprintf("SELECT tahun_per FROM (SELECT LEFT(periode,4) AS tahun_per FROM piutang WHERE flaglunas='0' AND flagangsur='0' GROUP BY LEFT(periode,4) UNION SELECT LEFT(periode,4) AS tahun_per FROM bayar%d WHERE flaglunas='1' AND flagangsur='0' GROUP BY LEFT(periode,4)) a GROUP BY tahun_per", tahun)
				rows2, err2 := dbbilling.QueryContext(ctx, qtahuns)
				if err2 != nil {
					log.Fatalf("Gagal ambil data : %v", err2)
				}
				defer rows2.Close()

				for rows2.Next() {
					err2 = rows2.Scan(&tahuns)
					if err2 != nil {
						log.Fatal("Data kosong : ", err2)
					}

					Callback_ConBill(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)

					fmt.Printf("AMBIL DATA BAYAR MANUAL TAHUN : %s       \r", tahuns)

					var q_piutang string
					// ctx_cek := context.Background()
					q_cek := fmt.Sprintf("SELECT * FROM bayar%d WHERE flaglunas='1' AND flagangsur='0' AND periode<'%s' AND LEFT(periode,4)='%s' AND DATE(tglbayar)>'%s'", tahun, data.Kode, tahuns, data.Tgllalu)
					rows, err := dbbilling.QueryContext(ctx, q_cek)
					if err != nil {
						log.Fatalf("Gagal ambil data : %v", err)
					}

					defer rows.Close()

					var cekisi string
					if rows.Next() {
						cekisi = "1"
					} else {
						cekisi = "0"
					}

					if cekisi == "1" {
						// ctx_piutang := context.Background()
						q_piutang = fmt.Sprintf("REPLACE INTO dumplaporanpiutang_manual SELECT CONCAT('SERVICE.',c.kode),'SERVICE', c.kode,c.periode,c.nosamb,c.bulan,c.kodegol,c.kodediameter,c.koderayon,c.stanlalu,c.stanskrg,c.pakai,c.prog1,c.prog2,c.prog3,c.prog4,c.biayapemakaian,c.administrasi,c.pemeliharaan,c.retribusi,c.meterai,c.rekair,0 AS denda,c.administrasilain,c.pemeliharaanlain,c.retribusilain,c.pelayanan,c.rekair+c.administrasilain+c.pemeliharaanlain+c.retribusilain+c.pelayanan  AS total,pakai2 FROM bayar%d  c WHERE c.flaglunas='1' AND c.flagangsur='0' AND c.periode<'%s' AND LEFT(c.periode,4)='%s' AND DATE(tglbayar)>'%s'", tahun, data.Kode, tahuns, data.Tgllalu)
						rows, err := dbbilling.QueryContext(ctx, q_piutang)
						if err != nil {
							log.Fatalf("Gagal ambil data : %v", err)
						}

						defer rows.Close()
					}

					fmt.Printf("AMBIL DATA BAYAR MANUAL TAHUN : %s [DONE]\r", tahuns)
					time.Sleep(500 * time.Millisecond)
					// }
				}
				fmt.Println("AMBIL DATA BAYAR MANUAL [DONE]              ")

				q_banyakkor := fmt.Sprint("SELECT COUNT(*) AS banyak FROM ba_koreksi_rekening WHERE DATE(tanggalba)>'", data.Tgllalu, "' AND nomorba LIKE '%DRD-KOREKSI%' AND flaghapus='0' AND periode<'", data.Kode, "'")
				rows, err = dbloket.QueryContext(ctx, q_banyakkor)
				if err != nil {
					log.Fatal("Gagal ambil data total : ", err)
				}
				defer rows.Close()

				ctx_koreksim := context.Background()
				q_koreksim := fmt.Sprint("SELECT CONCAT(periode,'.',nosamb) AS kode,nosamb,periode,bulan,kodegol,kodediameter,koderayon,COALESCE(stanlalu_lama,0),COALESCE(stankini_lama,0),COALESCE(pakai_lama,0),COALESCE(biayapemakaian_lama,0),COALESCE(administrasi_lama,0),COALESCE(pemeliharaan_lama,0),COALESCE(pelayanan_lama,0),COALESCE(retribusi_lama,0),COALESCE(meterai_lama,0),COALESCE(rekair_lama,0) FROM ba_koreksi_rekening WHERE DATE(tanggalba)>'", data.Tgllalu, "' AND nomorba LIKE '%DRD-KOREKSI%' AND flaghapus='0' AND periode<'", data.Kode, "' ORDER BY nosamb ASC,periode ASC,tanggalba DESC,nomorba DESC")
				rows_kore, ekor := dbloket.QueryContext(ctx_koreksim, q_koreksim)
				if ekor != nil {
					log.Fatalf("Gagal ambil data : %v", ekor)
				}
				defer rows_kore.Close()

				var totaldata string
				if rows.Next() {
					err := rows.Scan(&totaldata)

					if err != nil {
						log.Fatal("Data total kosong : ", err)
					}
				}

				var results []string
				for rows_kore.Next() {
					var kode, nosamb, periode, bulan, kodegol, kodediameter, koderayon string
					var stanlalu, stankini, pakai, biayapemakaian, administrasi, pemeliharaan, pelayanan, retribusi, meterai, rekair float64

					e_koreksim := rows_kore.Scan(&kode, &nosamb, &periode, &bulan, &kodegol, &kodediameter, &koderayon, &stanlalu, &stankini, &pakai, &biayapemakaian, &administrasi, &pemeliharaan, &pelayanan, &retribusi, &meterai, &rekair)

					if e_koreksim != nil {
						log.Fatal("Data kosong : ", e_koreksim)
					}
					results = append(results, kode)
					result := len(results)
					// fmt.Println(result, "/", totaldata)
					fmt.Printf("BALIK DATA KOREKSI MANUAL [%d/%s]\r", result, totaldata)

					Callback_ConBill(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)

					// ctx_upkoreksi := context.Background()
					q_upkoreksi := fmt.Sprintf("SELECT COALESCE(SUM(administrasilain),0) AS administrasilain,COALESCE(SUM(pemeliharaanlain),0) AS pemeliharaanlain,COALESCE(SUM(retribusilain),0) AS retribusilain,COALESCE(SUM(pakai2),0) AS pakai2 FROM dumplaporanpiutang_manual WHERE CONCAT(periode,'.',nosamb)='%s' AND user='SERVICE'", kode)
					rows, err = dbbilling.QueryContext(ctx, q_upkoreksi)
					if err != nil {
						log.Fatalf("Gagal ambil data update koreksi : %v", err)
					}

					defer rows.Close()

					var administrasilain, pemeliharaanlain, retribusilain, pakai2 float64
					var cekisi string
					if rows.Next() {
						cekisi = "1"
					} else {
						cekisi = "0"
					}
					if cekisi == "1" {
						for rows.Next() {

							err = rows.Scan(&administrasilain, &pemeliharaanlain, &retribusilain, &pakai2)

							if err != nil {
								log.Fatal("Data pakai2 kosong : ", err)
							}

							Callback_ConBill(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)

							// ctx_delkor := context.Background()
							q_delkor := fmt.Sprintf("DELETE FROM dumplaporanpiutang_manual WHERE CONCAT(periode,'.',nosamb)='%s' AND user='SERVICE'", kode)
							rows, err = dbbilling.QueryContext(ctx, q_delkor)
							if err != nil {
								log.Fatalf("Gagal hapus data koreksi : %v", err)
							}

							defer rows.Close()
							// fmt.Printf("BALIK DATA KOREKSI MANUAL [%d/%s] - 1 , %s\r", result, totaldata, kode)
						}
					}

					Callback_ConBill(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)

					// ctx_inskor := context.Background()
					q_inskor := fmt.Sprint("REPLACE INTO dumplaporanpiutang_manual VALUES (CONCAT('SERVICE','.','", kode, "'),'SERVICE','", kode, "','", periode, "','", nosamb, "','", bulan, "','", kodegol, "','", kodediameter, "','", koderayon, "',", stanlalu, ",", stankini, ",", pakai, ",0,0,0,0,", biayapemakaian, ",", administrasi, ",", pemeliharaan, ",", retribusi, ",", meterai, ",", rekair, ",0,", administrasilain, ",", pemeliharaanlain, ",", retribusilain, ",", pelayanan, ",", rekair+administrasilain+pemeliharaanlain+retribusilain+pelayanan, ",", pakai2, ")")
					rows, err = dbbilling.QueryContext(ctx, q_inskor)
					if err != nil {
						log.Fatalf("Gagal insert data koreksi : %v", err)
					}

					defer rows.Close()
					// fmt.Printf("BALIK DATA KOREKSI MANUAL [%d/%s] - 2 , %s\r", result, totaldata, kode)
				}
				// }
				fmt.Println("BALIK DATA KOREKSI MANUAL [DONE]                              ")

				fmt.Print("PROSES DRD ", strings.ToUpper(data.bulan))
				ctx_cekposting := context.Background()
				q_cekposting := fmt.Sprint("SHOW TABLES LIKE 'drdposting", data.Kode, "%'")
				cek_posting, eposting := dbbilling.QueryContext(ctx_cekposting, q_cekposting)
				if eposting != nil {
					log.Fatalf("Gagal ambil data : %v", eposting)
				}

				defer cek_posting.Close()

				var tabelposting string
				if cek_posting.Next() {
					tabelposting = "1"
				} else {
					tabelposting = "0"
				}

				ctx_cekreal := context.Background()
				q_cekreal := fmt.Sprint("SHOW TABLES LIKE 'drd", data.Kode, "%'")
				cek_real, ereal := dbbilling.QueryContext(ctx_cekreal, q_cekreal)
				if ereal != nil {
					log.Fatalf("Gagal ambil data : %v", eposting)
				}

				defer cek_real.Close()

				var tabelreal string
				if cek_real.Next() {
					tabelreal = "1"
				} else {
					tabelreal = "0"
				}

				var q_posting string
				if tabelposting == "1" {
					ctx_posting := context.Background()
					q_posting = fmt.Sprintf("REPLACE INTO dumplaporanpiutang_manual SELECT CONCAT('SERVICE.',CONCAT('%s','.',c.nosamb)) AS id,'SERVICE' AS USER, CONCAT('%s','.',c.nosamb) AS kode,'%s' AS periode,c.nosamb,'%s' AS bulan,c.kodegol,c.kodediameter,c.koderayon,c.stanlalu,c.stanskrg,c.pakai,c.prog1,c.prog2,c.prog3,c.prog4,c.biayapemakaian,c.administrasi,c.pemeliharaan,c.retribusi,c.meterai,c.rekair,0 AS denda,c.administrasilain,c.pemeliharaanlain,c.retribusilain,c.pelayanan,c.rekair+c.administrasilain+c.pemeliharaanlain+c.retribusilain+c.pelayanan  AS total,pakai2 FROM drdposting"+data.Kode+"  c  WHERE c.flagpublish='1'", data.Kode, data.Kode, data.Kode, data.bulan)
					posting, epos := dbbilling.QueryContext(ctx_posting, q_posting)
					if epos != nil {
						log.Fatalf("Gagal ambil data : %v", epos)
					}
					defer posting.Close()
				} else {
					if tabelreal == "1" {
						ctx_posting := context.Background()
						q_posting = fmt.Sprintf("REPLACE INTO dumplaporanpiutang_manual SELECT CONCAT('SERVICE.',CONCAT('%s','.',c.nosamb)) AS id,'SERVICE' AS USER, CONCAT('%s','.',c.nosamb) AS kode,'%s' AS periode,c.nosamb,'%s' AS bulan,c.kodegol,c.kodediameter,c.koderayon,c.stanlalu,c.stanskrg,c.pakai,c.prog1,c.prog2,c.prog3,c.prog4,c.biayapemakaian,c.administrasi,c.pemeliharaan,c.retribusi,c.meterai,c.rekair,0 AS denda,c.administrasilain,c.pemeliharaanlain,c.retribusilain,c.pelayanan,c.rekair+c.administrasilain+c.pemeliharaanlain+c.retribusilain+c.pelayanan  AS total,pakai2 FROM drd"+data.Kode+"  c  WHERE c.flagpublish='1'", data.Kode, data.Kode, data.Kode, data.bulan)
						posting, epos := dbbilling.QueryContext(ctx_posting, q_posting)
						if epos != nil {
							log.Fatalf("Gagal ambil data : %v", epos)
						}
						defer posting.Close()
					}
				}

				fmt.Println(" [DONE]")

				fmt.Print("PROSES LPP ", strings.ToUpper(data.bulan))
				ctx = context.Background()
				q_lpp := fmt.Sprint("UPDATE dumplaporanpiutang_manual a,(SELECT * FROM bayar", tahun, "  c WHERE c.flaglunas='1' AND c.flagangsur='0' AND DATE_FORMAT(tglbayar,'%Y%m')='", data.Kode, "') b SET a.stanlalu=a.stanlalu-b.stanlalu,a.stanskrg=a.stanlalu-b.stanlalu,a.pakai=a.pakai-b.pakai,a.biayapemakaian=a.biayapemakaian-b.biayapemakaian,a.administrasi=a.administrasi-b.administrasi,a.pemeliharaan=a.pemeliharaan-b.pemeliharaan,a.pelayanan=a.pelayanan-b.pelayanan,a.retribusi=a.retribusi-b.retribusi,a.meterai=a.meterai-b.meterai,a.rekair=a.rekair-b.rekair WHERE a.kode=b.kode")

				cek_lpp, elpp := dbbilling.QueryContext(ctx, q_lpp)
				if elpp != nil {
					log.Fatalf("Gagal ambil data : %v", elpp)
				}

				defer cek_lpp.Close()

				fmt.Println(" [DONE]")

				q_banyakkor = fmt.Sprint("SELECT COUNT(*) AS banyak FROM (SELECT * FROM ba_koreksi_rekening WHERE DATE_FORMAT(tanggalba,'%Y%m')='", data.Kode, "' AND nomorba LIKE '%DRD-KOREKSI%' AND flaghapus='0' AND periode='", data.Kode, "' GROUP BY nosamb,periode) a")
				rows, err = dbloket.QueryContext(ctx, q_banyakkor)
				if err != nil {
					log.Fatal("Gagal ambil data total : ", err)
				}
				defer rows.Close()

				var tot string
				if rows.Next() {
					err := rows.Scan(&tot)

					if err != nil {
						log.Fatal("Data total kosong : ", err)
					}
				}

				ctx_koreksim = context.Background()
				q_koreksim = fmt.Sprint("SELECT CONCAT(periode,'.',nosamb) AS kode,nosamb,periode,bulan,kodegol,kodediameter,koderayon,SUM(COALESCE(stanlalu_baru,0))-SUM(COALESCE(stanlalu_lama,0)),SUM(COALESCE(stankini_baru,0))-SUM(COALESCE(stankini_lama,0)),SUM(COALESCE(pakai_baru,0))-SUM(COALESCE(pakai_lama,0)),SUM(COALESCE(biayapemakaian_baru,0))-SUM(COALESCE(biayapemakaian_lama,0)),SUM(COALESCE(administrasi_baru,0))-SUM(COALESCE(administrasi_lama,0)),SUM(COALESCE(pemeliharaan_baru,0))-SUM(COALESCE(pemeliharaan_lama,0)),SUM(COALESCE(pelayanan_baru,0))-SUM(COALESCE(pelayanan_lama,0)),SUM(COALESCE(retribusi_baru,0))-SUM(COALESCE(retribusi_lama,0)),SUM(COALESCE(meterai_baru,0))-SUM(COALESCE(meterai_lama,0)),SUM(COALESCE(rekair_baru,0))-SUM(COALESCE(rekair_lama,0)) FROM ba_koreksi_rekening WHERE DATE_FORMAT(tanggalba,'%Y%m')='", data.Kode, "' AND nomorba LIKE '%DRD-KOREKSI%' AND flaghapus='0' AND periode<'", data.Kode, "' GROUP BY CONCAT(periode,'.',nosamb)")
				rows_kore, ekor = dbloket.QueryContext(ctx_koreksim, q_koreksim)
				if ekor != nil {
					log.Fatalf("Gagal ambil data : %v", ekor)
				}
				defer rows_kore.Close()

				var resultts2 []string
				for rows_kore.Next() {
					var kode, nosamb, periode, bulan, kodegol, kodediameter, koderayon string
					var stanlalu_selisih, stankini_selisih, pakai_selisih, biayapemakaian_selisih, administrasi_selisih, pemeliharaan_selisih, pelayanan_selisih, retribusi_selisih, meterai_selisih, rekair_selisih float64

					e_koreksim := rows_kore.Scan(&kode, &nosamb, &periode, &bulan, &kodegol, &kodediameter, &koderayon, &stanlalu_selisih, &stankini_selisih, &pakai_selisih, &biayapemakaian_selisih, &administrasi_selisih, &pemeliharaan_selisih, &pelayanan_selisih, &retribusi_selisih, &meterai_selisih, &rekair_selisih)

					if e_koreksim != nil {
						log.Fatal("Data kosong : ", e_koreksim)
					}
					resultts2 = append(resultts2, kode)
					resultt := len(resultts2)
					// fmt.Println(result, "/", totaldata)
					fmt.Printf("PROSES DATA KOREKSI MANUAL [%d/%s]\r", resultt, tot)

					Callback_ConBill(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)

					// ctx_upkoreksi := context.Background()
					q_upkoreksi := fmt.Sprintf("SELECT COALESCE(SUM(rekair),0) AS rekair,COALESCE(SUM(stanlalu),0) AS stanlalu,COALESCE(SUM(stanskrg),0) AS stanskrg,COALESCE(SUM(pakai),0) AS pakai,COALESCE(SUM(biayapemakaian),0) AS biayapemakaian,COALESCE(SUM(administrasi),0) AS administrasi,COALESCE(SUM(pemeliharaan),0) AS pemeliharaan,COALESCE(SUM(retribusi),0) AS retribusi,COALESCE(SUM(meterai),0) AS meterai,COALESCE(SUM(pelayanan),0) AS pelayanan, COALESCE(SUM(administrasilain),0) AS administrasilain,COALESCE(SUM(pemeliharaanlain),0) AS pemeliharaanlain,COALESCE(SUM(retribusilain),0) AS retribusilain,COALESCE(SUM(pakai2),0) AS pakai2 FROM dumplaporanpiutang_manual WHERE CONCAT(periode,'.',nosamb)='%s' AND user='SERVICE'", kode)
					rows, err = dbbilling.QueryContext(ctx, q_upkoreksi)
					if err != nil {
						log.Fatalf("Gagal ambil data update koreksi : %v", err)
					}

					defer rows.Close()

					var rekair, stanlalu, stankini, pakai, biayapemakaian, administrasi, pemeliharaan, retribusi, meterai, pelayanan, administrasilain, pemeliharaanlain, retribusilain, pakai2 float64
					// var cekisi string
					// if rows.Next() {
					// 	cekisi = "1"
					// } else {
					// 	cekisi = "0"
					// }
					// if cekisi == "1" {
					for rows.Next() {

						err = rows.Scan(&rekair, &stanlalu, &stankini, &pakai, &biayapemakaian, &administrasi, &pemeliharaan, &retribusi, &meterai, &pelayanan, &administrasilain, &pemeliharaanlain, &retribusilain, &pakai2)

						if err != nil {
							log.Fatal("Data pakai2 kosong : ", err)
						}

						Callback_ConBill(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)

						// ctx_delkor := context.Background()
						q_delkor := fmt.Sprintf("DELETE FROM dumplaporanpiutang_manual WHERE CONCAT(periode,'.',nosamb)='%s' AND user='SERVICE'", kode)
						rows, err = dbbilling.QueryContext(ctx, q_delkor)
						if err != nil {
							log.Fatalf("Gagal hapus data koreksi : %v", err)
						}

						defer rows.Close()
						// fmt.Printf("BALIK DATA KOREKSI MANUAL [%d/%s] - 1 , %s\r", result, totaldata, kode)
						Callback_ConBill(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)

						// ctx_inskor := context.Background()
						q_inskor := fmt.Sprint("REPLACE INTO dumplaporanpiutang_manual VALUES (CONCAT('SERVICE','.','", kode, "'),'SERVICE','", kode, "','", periode, "','", nosamb, "','", bulan, "','", kodegol, "','", kodediameter, "','", koderayon, "',", stanlalu+stanlalu_selisih, ",", stankini+stankini_selisih, ",", pakai+pakai_selisih, ",0,0,0,0,", biayapemakaian+biayapemakaian_selisih, ",", administrasi+administrasi_selisih, ",", pemeliharaan+pemeliharaan_selisih, ",", retribusi+retribusi_selisih, ",", meterai+meterai_selisih, ",", rekair+rekair_selisih, ",0,", administrasilain, ",", pemeliharaanlain, ",", retribusilain, ",", pelayanan+pelayanan_selisih, ",", (rekair+rekair_selisih)+administrasilain+pemeliharaanlain+retribusilain+(pelayanan+pelayanan_selisih), ",", pakai2, ")")
						rows, err = dbbilling.QueryContext(ctx, q_inskor)
						if err != nil {
							log.Fatalf("Gagal insert data koreksi : %v", err)
						}

						defer rows.Close()
					}
					// }

					// fmt.Printf("BALIK DATA KOREKSI MANUAL [%d/%s] - 2 , %s\r", result, totaldata, kode)
				}
				// }
				fmt.Println("PROSES DATA KOREKSI MANUAL [DONE]                              ")

				fmt.Println("- PROSES PIUTANG LAPORAN ", strings.ToUpper(data.bulan))

				qtahuns = fmt.Sprintf("SELECT tahun_per FROM (SELECT LEFT(periode,4) AS tahun_per FROM piutang WHERE flaglunas='0' AND flagangsur='0' GROUP BY LEFT(periode,4) UNION SELECT LEFT(periode,4) AS tahun_per FROM bayar%d WHERE flaglunas='0' AND flagangsur='0' GROUP BY LEFT(periode,4)) a GROUP BY tahun_per", tahun)
				rows1, err1 = dbbilling.QueryContext(ctx, qtahuns)
				if err1 != nil {
					log.Fatalf("Gagal ambil data : %v", err1)
				}
				defer rows1.Close()

				for rows1.Next() {
					err1 = rows1.Scan(&tahuns)
					if err1 != nil {
						log.Fatal("Data kosong : ", err1)
					}

					Callback_ConBill(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)

					fmt.Printf("AMBIL DATA PIUTANG LAPORAN TAHUN : %s       \r", tahuns)

					var q_piutang string
					// ctx_cek := context.Background()
					q_cek := fmt.Sprintf("SELECT * FROM piutang WHERE flaglunas='0' AND flagangsur='0' AND periode<='%s' AND LEFT(periode,4)='%s'", data.Kode, tahuns)
					rows, err := dbbilling.QueryContext(ctx, q_cek)
					if err != nil {
						log.Fatalf("Gagal ambil data : %v", err)
					}

					defer rows.Close()

					var cekisi string
					if rows.Next() {
						cekisi = "1"
					} else {
						cekisi = "0"
					}

					if cekisi == "1" {
						// ctx_piutang := context.Background()
						q_piutang = fmt.Sprintf("REPLACE INTO dumplaporanpiutang SELECT CONCAT('SERVICE.',c.kode),'SERVICE', c.kode,c.periode,c.nosamb,c.bulan,c.kodegol,c.kodediameter,c.koderayon,c.stanlalu,c.stanskrg,c.pakai,c.prog1,c.prog2,c.prog3,c.prog4,c.biayapemakaian,c.administrasi,c.pemeliharaan,c.retribusi,c.meterai,c.rekair,0 AS denda,c.administrasilain,c.pemeliharaanlain,c.retribusilain,c.pelayanan,c.rekair+c.administrasilain+c.pemeliharaanlain+c.retribusilain+c.pelayanan  AS total,pakai2 FROM piutang  c WHERE c.flaglunas='0' AND c.flagangsur='0' AND c.periode<='%s' AND LEFT(c.periode,4)='%s'", data.Kode, tahuns)
						rows, err := dbbilling.QueryContext(ctx, q_piutang)
						if err != nil {
							log.Fatalf("Gagal ambil data : %v", err)
						}

						defer rows.Close()
					}

					fmt.Printf("AMBIL DATA PIUTANG LAPORAN TAHUN : %s [DONE]\r", tahuns)
					time.Sleep(500 * time.Millisecond)
				}
				fmt.Println("AMBIL DATA PIUTANG LAPORAN [DONE]              ")

				// ctx_tahun = context.Background()
				qtahuns = fmt.Sprintf("SELECT tahun_per FROM (SELECT LEFT(periode,4) AS tahun_per FROM piutang WHERE flaglunas='0' AND flagangsur='0' GROUP BY LEFT(periode,4) UNION SELECT LEFT(periode,4) AS tahun_per FROM bayar%d WHERE flaglunas='1' AND flagangsur='0' GROUP BY LEFT(periode,4)) a GROUP BY tahun_per", tahun)
				rows2, err2 = dbbilling.QueryContext(ctx, qtahuns)
				if err2 != nil {
					log.Fatalf("Gagal ambil data : %v", err2)
				}
				defer rows2.Close()

				for rows2.Next() {
					err2 = rows2.Scan(&tahuns)
					if err2 != nil {
						log.Fatal("Data kosong : ", err2)
					}

					Callback_ConBill(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)

					fmt.Printf("AMBIL DATA BAYAR LAPORAN TAHUN : %s       \r", tahuns)

					var q_piutang string
					// ctx_cek := context.Background()
					q_cek := fmt.Sprintf("SELECT * FROM bayar%d WHERE flaglunas='1' AND flagangsur='0' AND periode<='%s' AND LEFT(periode,4)='%s' AND DATE(tglbayar)>'%s'", tahun, data.Kode, tahuns, data.Tglkini)
					rows, err := dbbilling.QueryContext(ctx, q_cek)
					if err != nil {
						log.Fatalf("Gagal ambil data : %v", err)
					}

					defer rows.Close()

					var cekisi string
					if rows.Next() {
						cekisi = "1"
					} else {
						cekisi = "0"
					}

					if cekisi == "1" {
						// ctx_piutang := context.Background()
						q_piutang = fmt.Sprintf("REPLACE INTO dumplaporanpiutang SELECT CONCAT('SERVICE.',c.kode),'SERVICE', c.kode,c.periode,c.nosamb,c.bulan,c.kodegol,c.kodediameter,c.koderayon,c.stanlalu,c.stanskrg,c.pakai,c.prog1,c.prog2,c.prog3,c.prog4,c.biayapemakaian,c.administrasi,c.pemeliharaan,c.retribusi,c.meterai,c.rekair,0 AS denda,c.administrasilain,c.pemeliharaanlain,c.retribusilain,c.pelayanan,c.rekair+c.administrasilain+c.pemeliharaanlain+c.retribusilain+c.pelayanan  AS total,pakai2 FROM bayar%d  c WHERE c.flaglunas='1' AND c.flagangsur='0' AND c.periode<='%s' AND LEFT(c.periode,4)='%s' AND DATE(tglbayar)>'%s'", tahun, data.Kode, tahuns, data.Tglkini)
						rows, err := dbbilling.QueryContext(ctx, q_piutang)
						if err != nil {
							log.Fatalf("Gagal ambil data : %v", err)
						}

						defer rows.Close()
					}

					fmt.Printf("AMBIL DATA BAYAR LAPORAN TAHUN : %s [DONE]\r", tahuns)
					time.Sleep(500 * time.Millisecond)
					// }
				}
				fmt.Println("AMBIL DATA BAYAR LAPORAN [DONE]              ")

				// q_sumrek := fmt.Sprintf("SELECT SUM(rekair) FROM dumplaporanpiutang WHERE user='SERVICE' AND kode NOT IN (SELECT kode FROM drdhapussecaraakuntansi WHERE tglhapussecaraakuntansi<='%s')", data.Tglkini)
				// rows, err = dbbilling.QueryContext(ctx, q_sumrek)
				// if err != nil {
				// 	log.Fatal("Gagal ambil data total : ", err)
				// }
				// defer rows.Close()

				// var jumrekair string
				// if rows.Next() {
				// 	err := rows.Scan(&jumrekair)

				// 	if err != nil {
				// 		log.Fatal("Data total kosong : ", err)
				// 	}
				// }
				// fmt.Println(jumrekair)

				q_banyakkor = fmt.Sprint("SELECT COUNT(*) AS banyak FROM (SELECT * FROM ba_koreksi_rekening WHERE DATE_FORMAT(tanggalba,'%Y%m')>'", data.Kode, "' AND nomorba LIKE '%DRD-KOREKSI%' AND flaghapus='0' AND periode<='", data.Kode, "') a")
				rows, err = dbloket.QueryContext(ctx, q_banyakkor)
				if err != nil {
					log.Fatal("Gagal ambil data total : ", err)
				}
				defer rows.Close()

				var tots string
				if rows.Next() {
					err := rows.Scan(&tots)

					if err != nil {
						log.Fatal("Data total kosong : ", err)
					}
				}

				ctx_koreksim = context.Background()
				q_koreksim = fmt.Sprint("SELECT CONCAT(periode,'.',nosamb) AS kode,nosamb,periode,bulan,kodegol,kodediameter,koderayon,SUM(COALESCE(stanlalu_baru,0))-SUM(COALESCE(stanlalu_lama,0)),SUM(COALESCE(stankini_baru,0))-SUM(COALESCE(stankini_lama,0)),SUM(COALESCE(pakai_baru,0))-SUM(COALESCE(pakai_lama,0)),SUM(COALESCE(biayapemakaian_baru,0))-SUM(COALESCE(biayapemakaian_lama,0)),SUM(COALESCE(administrasi_baru,0))-SUM(COALESCE(administrasi_lama,0)),SUM(COALESCE(pemeliharaan_baru,0))-SUM(COALESCE(pemeliharaan_lama,0)),SUM(COALESCE(pelayanan_baru,0))-SUM(COALESCE(pelayanan_lama,0)),SUM(COALESCE(retribusi_baru,0))-SUM(COALESCE(retribusi_lama,0)),SUM(COALESCE(meterai_baru,0))-SUM(COALESCE(meterai_lama,0)),SUM(COALESCE(rekair_baru,0))-SUM(COALESCE(rekair_lama,0)) FROM ba_koreksi_rekening WHERE DATE_FORMAT(tanggalba,'%Y%m')>'", data.Kode, "' AND nomorba LIKE '%DRD-KOREKSI%' AND flaghapus='0' AND periode<='", data.Kode, "' GROUP BY CONCAT(periode,'.',nosamb)")
				rows_kore, ekor = dbloket.QueryContext(ctx_koreksim, q_koreksim)
				if ekor != nil {
					log.Fatalf("Gagal ambil data : %v", ekor)
				}
				defer rows_kore.Close()

				var resultss []string
				for rows_kore.Next() {
					var kode, nosamb, periode, bulan, kodegol, kodediameter, koderayon string
					var stanlalu_selisih, stankini_selisih, pakai_selisih, biayapemakaian_selisih, administrasi_selisih, pemeliharaan_selisih, pelayanan_selisih, retribusi_selisih, meterai_selisih, rekair_selisih float64

					e_koreksim := rows_kore.Scan(&kode, &nosamb, &periode, &bulan, &kodegol, &kodediameter, &koderayon, &stanlalu_selisih, &stankini_selisih, &pakai_selisih, &biayapemakaian_selisih, &administrasi_selisih, &pemeliharaan_selisih, &pelayanan_selisih, &retribusi_selisih, &meterai_selisih, &rekair_selisih)

					if e_koreksim != nil {
						log.Fatal("Data kosong : ", e_koreksim)
					}
					resultss = append(resultss, kode)
					resultt := len(resultss)
					// fmt.Println(result, "/", totaldata)
					fmt.Printf("BALIK DATA KOREKSI LAPORAN [%d/%s]\r", resultt, tots)

					Callback_ConBill(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)

					// ctx_upkoreksi := context.Background()
					q_upkoreksi := fmt.Sprintf("SELECT COALESCE(SUM(rekair),0) AS rekair,COALESCE(SUM(stanlalu),0) AS stanlalu,COALESCE(SUM(stanskrg),0) AS stanskrg,COALESCE(SUM(pakai),0) AS pakai,COALESCE(SUM(biayapemakaian),0) AS biayapemakaian,COALESCE(SUM(administrasi),0) AS administrasi,COALESCE(SUM(pemeliharaan),0) AS pemeliharaan,COALESCE(SUM(retribusi),0) AS retribusi,COALESCE(SUM(meterai),0) AS meterai,COALESCE(SUM(pelayanan),0) AS pelayanan, COALESCE(SUM(administrasilain),0) AS administrasilain,COALESCE(SUM(pemeliharaanlain),0) AS pemeliharaanlain,COALESCE(SUM(retribusilain),0) AS retribusilain,COALESCE(SUM(pakai2),0) AS pakai2 FROM dumplaporanpiutang WHERE CONCAT(periode,'.',nosamb)='%s' AND user='SERVICE'", kode)
					rows, err = dbbilling.QueryContext(ctx, q_upkoreksi)
					if err != nil {
						log.Fatalf("Gagal ambil data update koreksi : %v", err)
					}

					defer rows.Close()

					var rekair, stanlalu, stankini, pakai, biayapemakaian, administrasi, pemeliharaan, retribusi, meterai, pelayanan, administrasilain, pemeliharaanlain, retribusilain, pakai2 float64
					// var cekisi string
					// if rows.Next() {
					// 	cekisi = "1"
					// } else {
					// 	cekisi = "0"
					// }
					// if cekisi == "1" {
					for rows.Next() {

						err = rows.Scan(&rekair, &stanlalu, &stankini, &pakai, &biayapemakaian, &administrasi, &pemeliharaan, &retribusi, &meterai, &pelayanan, &administrasilain, &pemeliharaanlain, &retribusilain, &pakai2)

						if err != nil {
							log.Fatal("Data pakai2 kosong : ", err)
						}

						Callback_ConBill(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)

						// ctx_delkor := context.Background()
						q_delkor := fmt.Sprintf("DELETE FROM dumplaporanpiutang WHERE CONCAT(periode,'.',nosamb)='%s' AND user='SERVICE'", kode)
						rows, err = dbbilling.QueryContext(ctx, q_delkor)
						if err != nil {
							log.Fatalf("Gagal hapus data koreksi : %v", err)
						}

						defer rows.Close()
						// fmt.Printf("BALIK DATA KOREKSI MANUAL [%d/%s] - 1 , %s\r", result, totaldata, kode)
						Callback_ConBill(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)

						// ctx_inskor := context.Background()
						q_inskor := fmt.Sprint("REPLACE INTO dumplaporanpiutang VALUES (CONCAT('SERVICE','.','", kode, "'),'SERVICE','", kode, "','", periode, "','", nosamb, "','", bulan, "','", kodegol, "','", kodediameter, "','", koderayon, "',", stanlalu-stanlalu_selisih, ",", stankini-stankini_selisih, ",", pakai-pakai_selisih, ",0,0,0,0,", biayapemakaian-biayapemakaian_selisih, ",", administrasi-administrasi_selisih, ",", pemeliharaan-pemeliharaan_selisih, ",", retribusi-retribusi_selisih, ",", meterai-meterai_selisih, ",", rekair-rekair_selisih, ",0,", administrasilain, ",", pemeliharaanlain, ",", retribusilain, ",", pelayanan-pelayanan_selisih, ",", (rekair-rekair_selisih)+administrasilain+pemeliharaanlain+retribusilain+(pelayanan-pelayanan_selisih), ",", pakai2, ")")
						rows, err = dbbilling.QueryContext(ctx, q_inskor)
						if err != nil {
							log.Fatalf("Gagal insert data koreksi : %v", err)
						}

						defer rows.Close()
					}
					// }

					// fmt.Printf("BALIK DATA KOREKSI MANUAL [%d/%s] - 2 , %s\r", result, totaldata, kode)
				}
				// }
				fmt.Println("BALIK DATA KOREKSI LAPORAN [DONE]                              ")

				// q_sumrek = fmt.Sprint("SELECT SUM(rekair_baru-rekair_lama) FROM ba_koreksi_rekening WHERE DATE_FORMAT(tanggalba,'%Y%m')>'", data.Kode, "' AND nomorba LIKE '%DRD-KOREKSI%' AND flaghapus='0' AND periode<='", data.Kode, "'")
				// rows, err = dbloket.QueryContext(ctx, q_sumrek)
				// if err != nil {
				// 	log.Fatal("Gagal ambil data total : ", err)
				// }
				// defer rows.Close()

				// if rows.Next() {
				// 	err := rows.Scan(&jumrekair)

				// 	if err != nil {
				// 		log.Fatal("Data total kosong : ", err)
				// 	}
				// }
				// fmt.Println(jumrekair)

				// q_sumrek = fmt.Sprintf("SELECT SUM(rekair) FROM dumplaporanpiutang WHERE user='SERVICE' AND kode NOT IN (SELECT kode FROM drdhapussecaraakuntansi WHERE tglhapussecaraakuntansi<='%s')", data.Tglkini)
				// rows, err = dbbilling.QueryContext(ctx, q_sumrek)
				// if err != nil {
				// 	log.Fatal("Gagal ambil data total : ", err)
				// }
				// defer rows.Close()

				// if rows.Next() {
				// 	err := rows.Scan(&jumrekair)

				// 	if err != nil {
				// 		log.Fatal("Data total kosong : ", err)
				// 	}
				// }
				// fmt.Println(jumrekair)

				fmt.Print("CEK & INSERT SELISIH ", strings.ToUpper(data.bulan))
				var result = ""

				pesan = pesan + "\nBULAN " + strings.ToUpper(data.bulan) + "\n\n"

				Callback_ConBill(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)

				// ctx_upkoreksi := context.Background()
				q_banding := fmt.Sprintf("SELECT * FROM (SELECT kode,SUM(rekair) AS rek FROM dumplaporanpiutang WHERE USER='SERVICE' AND kode NOT IN (SELECT kode FROM drdhapussecaraakuntansi WHERE DATE(tglhapussecaraakuntansi)<='%s') GROUP BY kode) a,(SELECT kode,SUM(rekair) AS rek FROM dumplaporanpiutang_service WHERE USER='SERVICE' AND kode NOT IN (SELECT kode FROM drdhapussecaraakuntansi WHERE DATE(tglhapussecaraakuntansi)<='%s') GROUP BY kode) b WHERE a.kode=b.kode AND a.rek<>b.rek", data.Tglkini, data.Tglkini)
				rows, err = dbbilling.QueryContext(ctx, q_banding)
				if err != nil {
					log.Fatalf("Gagal ambil data update koreksi : %v", err)
				}

				defer rows.Close()

				for rows.Next() {
					var kode, kodem string
					var rekair, rekairm float64

					e_banding := rows.Scan(&kode, &rekair, &kodem, &rekairm)

					if e_banding != nil {
						log.Fatal("Data banding piutang kosong : ", e_banding)
					}

					result = result + fmt.Sprintf("Kode : %s | Rekair Manual : %.2f | Rekair Laporan : %.2f", kode, rekairm, rekair)
					pesan = pesan + fmt.Sprintf("Kode : %s | Rekair Manual : %.2f | Rekair Laporan : %.2f", kode, rekairm, rekair)
				}
				// CEK PIUTANG
				// for _, datapiut := range piutang {
				// 	//for _, datapos := range posting {
				// 	for _, datakorek := range koreksi {
				// 		if datapiut.Nosamb == datakorek.Nosamb {
				// 			// if (((datapiut.Awal_Piutang+datapiut.Awal_Bayar-datakorek.Awal1)+datapiut.Posting)-datapiut.Lpp)+datakorek.Awal2 != ((datapiut.Akhir_Piutang+datapiut.Akhir_Bayar)-datapiut.Lpp)-datakorek.Akhir {
				// 			if ((((datapiut.Awal_Piutang+datapiut.Awal_Bayar-datakorek.Awal1)+datapiut.Posting)-datapiut.Lpp)+datakorek.Awal2)-(((datapiut.Akhir_Piutang+datapiut.Akhir_Bayar)-datapiut.Lpp)-datakorek.Akhir) != 0 {
				// 				result = result + fmt.Sprintf("Nosamb : %s | Sisa Piutang Per %s : %.2f, DRD Posting %s : %.2f, LPP %s : %.2f, Koreksi Diatas %s : %.2f, Piutang Manual : %.2f | Sisa Piutang Per %s : %.2f, Koreksi Diatas %s : %.2f, Piutang Laporan : %.2f | Selisih : %.2f\n", datapiut.Nosamb, data.Tgllalu, (datapiut.Awal_Piutang+datapiut.Awal_Bayar-datakorek.Awal1), data.Kode, datapiut.Posting, data.bulan, datapiut.Lpp, data.Tgllalu, datakorek.Awal2, (((datapiut.Awal_Piutang+datapiut.Awal_Bayar-datakorek.Awal1)+datapiut.Posting)-datapiut.Lpp)+datakorek.Awal2, data.Tglkini, (datapiut.Akhir_Piutang+datapiut.Akhir_Bayar), data.Tglkini, datakorek.Akhir, ((datapiut.Akhir_Piutang+datapiut.Akhir_Bayar)-datapiut.Lpp)-datakorek.Akhir, ((((datapiut.Awal_Piutang+datapiut.Awal_Bayar-datakorek.Awal1)+datapiut.Posting)-datapiut.Lpp)+datakorek.Awal2)-(((datapiut.Akhir_Piutang+datapiut.Akhir_Bayar)-datapiut.Lpp)-datakorek.Akhir))
				// 				pesan = pesan + fmt.Sprintf("Nosamb : %s | Sisa Piutang Per %s : %.2f, DRD Posting %s : %.2f, LPP %s : %.2f, Koreksi Diatas %s : %.2f, Piutang Manual : %.2f | Sisa Piutang Per %s : %.2f, Koreksi Diatas %s : %.2f, Piutang Laporan : %.2f | Selisih : %.2f\n", datapiut.Nosamb, data.Tgllalu, (datapiut.Awal_Piutang+datapiut.Awal_Bayar-datakorek.Awal1), data.Kode, datapiut.Posting, data.bulan, datapiut.Lpp, data.Tgllalu, datakorek.Awal2, (((datapiut.Awal_Piutang+datapiut.Awal_Bayar-datakorek.Awal1)+datapiut.Posting)-datapiut.Lpp)+datakorek.Awal2, data.Tglkini, (datapiut.Akhir_Piutang+datapiut.Akhir_Bayar), data.Tglkini, datakorek.Akhir, ((datapiut.Akhir_Piutang+datapiut.Akhir_Bayar)-datapiut.Lpp)-datakorek.Akhir, ((((datapiut.Awal_Piutang+datapiut.Awal_Bayar-datakorek.Awal1)+datapiut.Posting)-datapiut.Lpp)+datakorek.Awal2)-(((datapiut.Akhir_Piutang+datapiut.Akhir_Bayar)-datapiut.Lpp)-datakorek.Akhir))
				// 			}
				// 		}
				// 	}
				// 	//}
				// }
				fmt.Println(" [DONE]")

				// fmt.Println(result)

				if result == "" {
					fmt.Printf("TIDAK ADA SELISIH BULAN %s [DONE]\n", strings.ToUpper(data.bulan))
					goto Sleeps
				} else {
					fmt.Printf("TERDAPAT SELISIH BULAN %s [DONE]\n", strings.ToUpper(data.bulan))
					break
				}
			Sleeps:
				time.Sleep(5 * time.Second)
			}

		} else {
			pesan = pesan + fmt.Sprintf("TABEL bayar %d TIDAK DITEMUKAN", tahun)
		}

	}

	return pesan
}

func readJSONArray(jsonString string) ([]Data, error) {
	var dataList []Data

	// Mengonversi string JSON ke dalam slice dari struktur data
	err := json.Unmarshal([]byte(jsonString), &dataList)
	if err != nil {
		return dataList, err
	}

	return dataList, nil
}

func Callback_ConBill(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill string) error {
	// fmt.Println(User_bill, ",", Pass_bill, ",", DB_bill, ",", Ip_bill, ",", Port_bill)
	dbbilling := connmaster.ConnBilling(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)
	defer dbbilling.Close()

	err := dbbilling.Ping()
	if err != nil {
		log.Fatalf("Tidak dapat terhubung ke database: %v", err)
	}

	// for {
	// 	dbbilling := connmaster.ConnBilling(User_bill, Pass_bill, DB_bill, Ip_bill, Port_bill)
	// 	defer dbbilling.Close()

	// 	// Mengecek koneksi ke database
	// 	err := dbbilling.Ping()
	// 	if err != nil {
	// 		fmt.Println("Tidak dapat terhubung ke database:", err)
	// 		// pesan = "Tidak dapat terhubung ke database:" + err.Error()
	// 		// telebot.TeleBot(pesan)
	// 		goto Sleeps
	// 	} else {

	// 		break

	// 	}
	// Sleeps:
	// 	time.Sleep(1 * time.Second)
	// }
	return nil
}

func getPelangganBill(dbbill *sql.DB) ([]NosambBill, error) {
	ctx_pelbill := context.Background()

	pelanggan_bill, e_pelbill := dbbill.QueryContext(ctx_pelbill, "SELECT nosamb FROM pelanggan WHERE flaghapus=0 ORDER BY `status`,nosamb")
	if e_pelbill != nil {
		log.Fatalf("Gagal ambil data : %v", e_pelbill)
	}

	defer pelanggan_bill.Close()

	var nosambBill []NosambBill

	for pelanggan_bill.Next() {
		var pelanggan NosambBill
		e_nosamb := pelanggan_bill.Scan(&pelanggan.Nosamb)
		if e_nosamb != nil {
			log.Fatal("Data kosong :", e_nosamb)
		}
		nosambBill = append(nosambBill, pelanggan)

	}

	defer pelanggan_bill.Close()
	//fmt.Println(&nosambBill)

	return nosambBill, nil
}

func getPelangganLoket(dbloket *sql.DB, nosambBill []NosambBill, pesan string) (string, error) {
	// Simulasi penggunaan data dari SELECT pertama untuk SELECT kedua
	// Di sini, kita hanya menggabungkan ID dari hasil SELECT pertama

	var nosambLoket []string
	for _, pelanggan := range nosambBill {
		nosambLoket = append(nosambLoket, pelanggan.Nosamb)

	}

	query := fmt.Sprintf("SELECT nosamb,nama FROM pelanggan WHERE nosamb NOT IN ('%s') AND flaghapus=0", strings.Join(nosambLoket, "','"))

	//mt.Println(query)
	ctx_pelloket := context.Background()

	pelanggan_loket, e_pelloket := dbloket.QueryContext(ctx_pelloket, query)
	// pelanggan_loket, e_pelloket := dbloket.Query(query)
	if e_pelloket != nil {
		log.Fatalf("Gagal ambil data pelanggan loket : %v", e_pelloket)
	}

	var dataPelLoket []NosambLoket
	for pelanggan_loket.Next() {
		var pelloket NosambLoket
		e_loket := pelanggan_loket.Scan(&pelloket.Nosamb, &pelloket.Nama)
		if e_loket != nil {
			log.Fatal("Data kosong :", e_loket)
		}
		dataPelLoket = append(dataPelLoket, pelloket)
	}

	fmt.Println(" [DONE]")
	fmt.Print("INSERT PESAN CEK SELISIH PELANGGAN")

	var result, pesan2 string
	for _, data := range dataPelLoket {
		result = fmt.Sprintf("%s - %s\n", data.Nosamb, data.Nama)
		//fmt.Printf("%s - %s\n", data.Nosamb, data.Nama)
	}

	if result == "" {
		pesan = pesan + "TIDAK ADA SELISIH PELANGGAN\n"
		pesan2 = " => TIDAK ADA SELISIH PELANGGAN"
	} else {
		pesan = pesan + result
		pesan2 = " => TERDAPAT SELISIH PELANGGAN"
	}
	fmt.Println(pesan2 + " [DONE]")

	return pesan, nil
}

// func CekConnLoket(User_lokets, Pass_lokets, DB_lokets, Ip_lokets, Port_lokets string) *sql.DB {
// 	// User_loket := User_lokets
// 	// Pass_loket := Pass_lokets
// 	// DB_loket := DB_lokets
// 	// Ip_loket := Ip_lokets
// 	// Port_loket := Port_lokets

// 	// return dbloket
// }
