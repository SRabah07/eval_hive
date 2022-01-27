for file in 'title.akas.tsv.gz' 'title.basics.tsv.gz' 'title.crew.tsv.gz' 'title.episode.tsv.gz' 'title.principals.tsv.gz' 'title.ratings.tsv.gz' 'name.basics.tsv.gz'; 
do
    echo "Download Dataset of $file"
    wget https://datasets.imdbws.com/$file
    gunzip $file
done

echo "Move all dataset to "data" volume used by docker"
mv *.tsv data/

ls -lrt data
echo "done!"
