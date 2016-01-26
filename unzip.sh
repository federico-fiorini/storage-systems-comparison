#!/bin/bash

if [ $1 = "--help" ]; then
    printf "\n\tUnzip all the file in the current directory.\n\tWith --keep option it will keep the zip files, otherwise they will be deleted\n\n"
    exit
fi

k=0
for i in *.zip; do
    mkdir "$i-dir"
    cd "$i-dir"
    unzip "../$i"
    for j in *; do
	mv "$j" "../${i%%_*}$k.${j##*.}"
    done
    cd ..
    ((k++))
done

# Remove dirs
rm -Rf ./*.zip-dir

# Remove
if ! [ $1 = "--keep" ]; then
    rm -Rf ./*.zip
fi
