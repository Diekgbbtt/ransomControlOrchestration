#!/bin/bash

# impostazione variabili per assicurarsi che shell trovi sqlcl binary - definire var ambiente ORACLE_HOME e aggiungere a PATH oracle_home/bin credo


if ! command -v sql &> /dev/null ; then
    echo "sqlcl non trovato" ; exit 1;
fi

if ! command -v zip &> /dev/null ; then
    echo "zip cmd non trovato" ; exit 1;
fi


if [[ $# -ne 4 && $# -ne 5 ]]; then
    echo "Usage: $0 <DB_HOSTNAME> <DB_PORT> <DB_USERNAME> <DB_PWD> [<DB_SID>]"; exit 1;
fi

timestamp=$(date | sed -e 's/^/_/' -e 's/     /_/' -e 's/ /_/g' -e 's/:/_/g')


function getResults() {

    #controllo / sanitificazione valori passati

DB_USERNAME=$3
DB_PWD=$4
DB_HOSTNAME=$1
DB_PORT=$2
if [ -n "$5" ]; then
    DB_SID=$5
    CONNECTION="sql -S $DB_USERNAME/$DB_PWD@$DB_HOSTNAME:$DB_PORT/$DB_SID";
else 
    CONNECTION="sql -S $DB_USERNAME/$DB_PWD@$DB_HOSTNAME:$DB_PORT";
fi


QUERIES="SET ECHO OFF
        SET FEEDBACK OFF
        -- SET COLSEP ','
        SET PAGESIZE 0
        SET SQLFORMAT DELIMITED ,
        SELECT DB_NAME, TABLE_NAME, COLUMN_NAME, VALUE, result, RES_ATTESO, 
        CASE WHEN CAST(result AS VARCHAR(200)) = RES_ATTESO THEN 1 ELSE 0 END AS EVALUATION
            FROM CHECK_BASE CB LEFT JOIN (
            WITH numbers AS(
                SELECT LEVEL AS n
                FROM DUAL
                CONNECT BY LEVEL <= 1000)
                SELECT ID, DATABASE_ID, TABLE_ID, COLUMN_ID, REGEXP_SUBSTR(REGEXP_SUBSTR(RESULT, '\"(\w+|\d+)\":', 1, n), '(\w+|\d+)') as chiavi, 
                    REGEXP_SUBSTR(REGEXP_SUBSTR(RESULT, ':\"(\w+|\d+)\"[,}]', 1, n), '(\w+|\d+)') as result
                    FROM CHECK_2, numbers
                    WHERE  n <= REGEXP_COUNT(RESULT, '\"(\w+|\d+)\":')
                    ORDER BY ID, n ) CR ON CB.VALUE = CAST(CR.chiavi AS VARCHAR(200)) 
                                                LEFT JOIN CHECK_LINK CL ON CB.ID = CL.ID_BASE 
                                                    JOIN CHECK_VIEW_2 CV2 ON CL.ID_CHECK = CV2.ID;
        EXIT"

$CONNECTION <<EOF
$QUERIES
EOF
}


evaluateResults() {


    
    # back up files dopo un determinato tempo oppure dimensione della cartella contenente i file, stabilito da una politica
    # ls -la /Evaluation | awk -F" "  '{sum($5)} ' | tee

    sed 's/""/"0"/' | awk -F,  '
              {if ($7 == 0) {
                    print "descrepancy revealed in database "$1" table "$2" column "$3". Value "$4" has ", ("$5" == "" ? 0 : $5 ), "occurrences while the expected occurrences are "$6""; 
                }
            }' | tee Evaluation/discrepancies$timestamp.txt
    # creazione dinamica del nome del file includendo data&ora per permettere di comprendere meglio l'intrusione.
    zip Evaluation/discrepancies$timestamp.zip Evaluation/discrepancies$timestamp.txt
    rm Evaluation/discrepancies$timestamp.txt

}


getResults $1 $2 $3 $4 $5 | evaluateResults










