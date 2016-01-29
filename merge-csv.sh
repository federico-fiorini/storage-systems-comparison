#!/bin/bash

if [ $# -eq 0 ]; then
  printf "\t\nMissing arguments. \n\n"
  exit
fi

OutFileName="$1.csv"
i=0
for filename in $1*.csv; do 
 if [ "$filename"  != "$OutName" ] ;
 then 
   if [[ $i -eq 0 ]] ; then 
      head -1  $filename >   $OutFileName
   fi
   tail -n +2  $filename >>  $OutFileName
   i=$(( $i + 1 ))
 fi
 rm $filename
done
