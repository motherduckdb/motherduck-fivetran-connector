Data source: https://www.datablist.com/learn/csv/download-sample-csv-files#download-customers-sample-csv-files

Encrypted using this command:
```bash
KEY=$(openssl rand -hex 32)
IV=$(openssl rand -hex 16)
echo -n $KEY | xxd -r -p > customers-100000.key
echo -n $IV | xxd -r -p > customers-100000.csv.enc
openssl enc -aes-256-cbc -in customers-100000.csv -K $KEY -iv $IV -nosalt >> customers-100000.csv.enc
```