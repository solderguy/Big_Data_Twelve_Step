# 
# CREDENTIALS
#   Module: S2_Mapper.sh  
#   Author: John Soper
#   Date: Apr 2015
#   Rev: 1
# 
# SUMMARY
#     This is the second component of the Big Data Twelve Step Program project  
#     A Hadoop streaming job uses this as as the map component 
#     (also with IdentityReducer)
#     It uses grep regular expressions to remove all partial JSON records
# 

tr -d '[:punct:]' | tr '[:upper:]' '[:lower:]' | 
grep 'point \+sn \+[1-9][0-9]* \+' |
grep 'x \+[0-9][0-9]*\|x \+null \+' |
grep 'y \+[0-9][0-9]* \+\|y \+null \+' |
tr -s '[:blank:]' ',' |
cut -d',' -f 3,5,7
