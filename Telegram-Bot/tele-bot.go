package telebot

import (
	"log"
	"os"

	tgbotapi "gopkg.in/telegram-bot-api.v4"
)

func TeleBot(pesan, nama_pdam string) {
	// Ganti token_bot_anda dengan token bot Telegram yang diberikan oleh BotFather
	bot, err := tgbotapi.NewBotAPI("6721892175:AAFgn2GAfgERuZtF5vaDxHlXJFauqX3nriM")
	if err != nil {
		log.Panic(err)
	}

	// Konfigurasi bot dengan menggunakan Debug = true untuk melihat log
	bot.Debug = true

	log.Printf("Authorized on account %s", bot.Self.UserName)
	//chatID := int64(-4001506424)
	chatID := int64(-4001506424)
	// // Pesan yang akan dikirim
	// msg := tgbotapi.NewMessage(chatID, pesan)

	// // Mengirim pesan
	// _, err = bot.Send(msg)
	// if err != nil {
	// 	log.Panic(err)
	// }

	// Buat file teks sementara untuk menyimpan output
	tempFilePath := "temp_output.txt"
	err = createTextFile(tempFilePath, pesan)
	if err != nil {
		log.Panic(err)
	}
	defer deleteFile(tempFilePath)

	// Baca file teks
	file, err := os.Open(tempFilePath)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	// Buat objek tgbotapi.File untuk file teks
	fileConfig := tgbotapi.FileBytes{
		Name:  nama_pdam + ".txt",
		Bytes: getFileBytes(file),
	}

	// Konfigurasi pesan dengan file
	msg := tgbotapi.NewDocumentUpload(chatID, fileConfig)

	// Kirim pesan dengan file
	_, err = bot.Send(msg)
	if err != nil {
		log.Panic(err)
	}
}

// createTextFile membuat file teks dengan isi yang diberikan
func createTextFile(filePath, content string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(content)
	return err
}

// deleteFile menghapus file
func deleteFile(filePath string) {
	err := os.Remove(filePath)
	if err != nil {
		log.Println("Gagal menghapus file:", err)
	}
}

// getFileBytes membaca file dan mengembalikan bytes-nya
func getFileBytes(file *os.File) []byte {
	fileInfo, _ := file.Stat()
	size := fileInfo.Size()
	bytes := make([]byte, size)

	_, err := file.Read(bytes)
	if err != nil {
		log.Panic(err)
	}

	return bytes
}
