echo "Download Dataset..."
wget https://datasets.imdbws.com/name.basics.tsv.gz

echo "Unzip Dataset..."
gunzip name.basics.tsv.gz

echo "Move dataset to 'data' volume used by docker"
mv name.basics.tsv data/

ls -lrt data
echo "done!"
