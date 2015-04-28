tr -d '[:punct:]' | tr '[:upper:]' '[:lower:]' | 
grep 'point \+sn \+[1-9][0-9]* \+' |
grep 'x \+[0-9][0-9]*\|x \+null \+' |
grep 'y \+[0-9][0-9]* \+\|y \+null \+' |
tr -s '[:blank:]' ',' |
cut -d',' -f 3,5,7
