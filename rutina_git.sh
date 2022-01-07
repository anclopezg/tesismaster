#! /bin/bash


cd /home/servidores/tesismaster
DIA_NUM=$(date +%y%m%d)

git add .
git commit -m $DIA_NUM
git push
