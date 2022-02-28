AUTHORS.txt: .mailmap
	go install github.com/kevinburke/write_mailmap@latest
	write_mailmap > AUTHORS.txt
