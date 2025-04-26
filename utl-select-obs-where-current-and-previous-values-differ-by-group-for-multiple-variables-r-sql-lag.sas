%let pgm=utl-select-obs-where-current-and-previous-values-differ-by-group-for-multiple-variables-r-sql-lag;

%stop_submission;

select records where current and previous values differt by group for multiple variables sql r python excel

github
https://tinyurl.com/xxe3dyty
https://github.com/rogerjdeangelis/utl-select-obs-where-current-and-previous-values-differ-by-group-for-multiple-variables-r-sql-lag

related to
communities.sas
https://tinyurl.com/33twv92k
https://communities.sas.com/t5/SAS-Programming/How-to-compare-the-record-with-previous-one-to-make-change-log/m-p/822197#M324651

SOAPBOX ON
  Repetive code can often be faster then looping and
  is ofen more understandable and maintainable.
  Also note iso dates sort properly in both vharacter and numeric formats.
SOAPBOX OFF

/**************************************************************************************************************************/
/*        INPUT                |          PROCESS                        |           OUTPUT                               */
/*        =====                |          =======                        |           ======                               */
/*                             |                                         |                                                */
/*    DATE    ID  TYPE NM COST | select the records where NM changes     |                                                */
/*                             | this sql code works in python and excel |Note the change at 2022-01-01 1A2 is not present*/
/* 2022-01-01 1A1  AA  BOLT 3  |                                         |because it is the first record for new group    */
/* 2022-01-02 1A1  AA  BOLT 3  |                                         |                                                */
/* 2022-01-03 1A1  AA  BOLT 4  |  DATE    ID  TYPE COST     NM           | > want                                         */
/* 2022-01-04 1A1  AA  JUMP 4  |                                         |     grp      date   id curval lagval           */
/* 2022-01-05 1A1  AA  BOLT 4  | 2022-01-01 1A1  AA   3    BOLT          |                                                */
/* 2022-01-06 1A1  BB  BOLT 4  | 2022-01-02 1A1  AA   3    BOLT          |   1  cost 2022-01-03 1A1      4      3         */
/* 2022-01-07 1A1  BB  BOLT 4  | 2022-01-03 1A1  AA   4    BOLT          |   2  cost 2022-01-12 1A1      5      4         */
/* 2022-01-08 1A1  BB  BOLT 4  | 2022-01-04 1A1  AA   4    JUMP *        |   3  cost 2022-01-03 1A2      4      3         */
/* 2022-01-09 1A1  BB  JUMP 4  | 2022-01-05 1A1  AA   4    BOLT *        |                                                */
/* 2022-01-10 1A1  BB  BOLT 4  | 2022-01-06 1A1  BB   4    BOLT          |   4    nm 2022-01-04 1A1   JUMP   BOLT         */
/* 2022-01-11 1A1  BB  BOLT 4  | 2022-01-07 1A1  BB   4    BOLT          |   5    nm 2022-01-05 1A1   BOLT   JUMP         */
/* 2022-01-12 1A1  CC  XTRA 5  | 2022-01-08 1A1  BB   4    BOLT          |   6    nm 2022-01-09 1A1   JUMP   BOLT         */
/* 2022-01-13 1A1  CC  BOLT 5  | 2022-01-09 1A1  BB   4    JUMP *        |   7    nm 2022-01-10 1A1   BOLT   JUMP         */
/* 2022-01-14 1A1  CC  BOLT 5  | 2022-01-10 1A1  BB   4    BOLT *        |   8    nm 2022-01-12 1A1   XTRA   BOLT         */
/* 2022-01-01 1A2  AA  XTRA 3  | 2022-01-11 1A1  BB   4    BOLT          |   9    nm 2022-01-13 1A1   BOLT   XTRA         */
/* 2022-01-02 1A2  AA  BOLT 3  | 2022-01-12 1A1  CC   5    XTRA *        |   10   nm 2022-01-02 1A2   BOLT   XTRA         */
/* 2022-01-03 1A2  AA  XTRA 4  | 2022-01-13 1A1  CC   5    BOLT *        |   11   nm 2022-01-03 1A2   XTRA   BOLT         */
/* 2022-01-10 1A2  AA  BOLT 4  | 2022-01-14 1A1  CC   5    BOLT          |   12   nm 2022-01-10 1A2   BOLT   XTRA         */
/*                             | 2022-01-01 1A2  AA   3    XTRA no new ID|                                                */
/* options validvarname=upcase;| 2022-01-02 1A2  AA   3    BOLT *        |   13 type 2022-01-06 1A1     BB     AA         */
/* libname sd1 "d:/sd1";       | 2022-01-03 1A2  AA   4    XTRA *        |   14 type 2022-01-12 1A1     CC     BB         */
/* data sd1.have;              | 2022-01-10 1A2  AA   4    BOLT *        |                                                */
/* input date $10. ID $        |                                         |                                                */
/*   TYPE $ NM $ COST $;       |----------------------------------------------------------------------------------------- */
/* cards4;                     |                                                                                          */
/* 2022-01-01 1A1 AA BOLT 3    | R SQL (SAME CODE IN PYTHON AND EXCEL)                                                    */
/* 2022-01-02 1A1 AA BOLT 3    | =====================================                                                    */
/* 2022-01-03 1A1 AA BOLT 4    |                                                                                          */
/* 2022-01-04 1A1 AA JUMP 4    | %utl_rbeginx;                                                                            */
/* 2022-01-05 1A1 AA BOLT 4    | parmcards4;                                                                              */
/* 2022-01-06 1A1 BB BOLT 4    | library(haven)                                                                           */
/* 2022-01-07 1A1 BB BOLT 4    | library(sqldf)                                                                           */
/* 2022-01-08 1A1 BB BOLT 4    | source("c:/oto/fn_tosas9x.R")                                                            */
/* 2022-01-09 1A1 BB JUMP 4    | options(sqldf.dll = "d:/dll/sqlean.dll")                                                 */
/* 2022-01-10 1A1 BB BOLT 4    | have<-read_sas("d:/sd1/have.sas7bdat")                                                   */
/* 2022-01-11 1A1 BB BOLT 4    | print(have)                                                                              */
/* 2022-01-12 1A1 CC XTRA 5    | want<-sqldf("                                                                            */
/* 2022-01-13 1A1 CC BOLT 5    | with                                                                                     */
/* 2022-01-14 1A1 CC BOLT 5    |   cost as ( select 'cost' as grp, date, id, cost as curval                               */
/* 2022-01-01 1A2 AA XTRA 3    |   ,lag(cost) over (partition by id  order by id, date) as lagval                         */
/* 2022-01-02 1A2 AA BOLT 3    |    from have),                                                                           */
/* 2022-01-03 1A2 AA XTRA 4    |  type as ( select 'type' as grp, date, id, type as curval                                */
/* 2022-01-10 1A2 AA BOLT 4    |   ,lag(type) over (partition by id  order by id, date) as lagval                         */
/* ;;;;                        |    from have),                                                                           */
/* run;quit;                   |  nm as ( select 'nm' as grp, date, id, nm as curval                                      */
/*                             |   ,lag(nm) over (partition by id  order by id, date) as lagval                           */
/*                             |    from have)                                                                            */
/*                             | select * from cost where curval != lagval                                                */
/*                             | union all select * from type where curval != lagval                                      */
/*                             | union all select * from nm   where curval != lagval                                      */
/*                             | order by grp, id, date                                                                   */
/*                             | ")                                                                                       */
/*                             | want                                                                                     */
/*                             | fn_tosas9x(                                                                              */
/*                             |       inp    = want                                                                      */
/*                             |      ,outlib ="d:/sd1/"                                                                  */
/*                             |      ,outdsn ="want"                                                                     */
/*                             |      )                                                                                   */
/*                             | ;;;;                                                                                     */
/*                             | %utl_rendx;                                                                              */
/***************************************************************************************************************************/

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
input date $10. ID $
  TYPE $ NM $ COST $;
cards4;
2022-01-01 1A1 AA BOLT 3
2022-01-02 1A1 AA BOLT 3
2022-01-03 1A1 AA BOLT 4
2022-01-04 1A1 AA JUMP 4
2022-01-05 1A1 AA BOLT 4
2022-01-06 1A1 BB BOLT 4
2022-01-07 1A1 BB BOLT 4
2022-01-08 1A1 BB BOLT 4
2022-01-09 1A1 BB JUMP 4
2022-01-10 1A1 BB BOLT 4
2022-01-11 1A1 BB BOLT 4
2022-01-12 1A1 CC XTRA 5
2022-01-13 1A1 CC BOLT 5
2022-01-14 1A1 CC BOLT 5
2022-01-01 1A2 AA XTRA 3
2022-01-02 1A2 AA BOLT 3
2022-01-03 1A2 AA XTRA 4
2022-01-10 1A2 AA BOLT 4
;;;;
run;quit;

/**************************************************************************************************************************/
/*  SD1.HAVE total obs=18                                                                                                 */
/*                                                                                                                        */
/*     DATE       ID     TYPE     NM     COST                                                                             */
/*                                                                                                                        */
/*  2022-01-01    1A1     AA     BOLT     3                                                                               */
/*  2022-01-02    1A1     AA     BOLT     3                                                                               */
/*  2022-01-03    1A1     AA     BOLT     4                                                                               */
/*  2022-01-04    1A1     AA     JUMP     4                                                                               */
/*  2022-01-05    1A1     AA     BOLT     4                                                                               */
/*  2022-01-06    1A1     BB     BOLT     4                                                                               */
/*  2022-01-07    1A1     BB     BOLT     4                                                                               */
/*  2022-01-08    1A1     BB     BOLT     4                                                                               */
/*  2022-01-09    1A1     BB     JUMP     4                                                                               */
/*  2022-01-10    1A1     BB     BOLT     4                                                                               */
/*  2022-01-11    1A1     BB     BOLT     4                                                                               */
/*  2022-01-12    1A1     CC     XTRA     5                                                                               */
/*  2022-01-13    1A1     CC     BOLT     5                                                                               */
/*  2022-01-14    1A1     CC     BOLT     5                                                                               */
/*  2022-01-01    1A2     AA     XTRA     3                                                                               */
/*  2022-01-02    1A2     AA     BOLT     3                                                                               */
/*  2022-01-03    1A2     AA     XTRA     4                                                                               */
/*  2022-01-10    1A2     AA     BOLT     4                                                                               */
/**************************************************************************************************************************/

/*
 _ __  _ __ ___   ___ ___  ___ ___
| `_ \| `__/ _ \ / __/ _ \/ __/ __|
| |_) | | | (_) | (_|  __/\__ \__ \
| .__/|_|  \___/ \___\___||___/___/
|_|
*/

proc datasets lib=sd1 nolist nodetails;
 delete want;
run;quit;

%utl_rbeginx;
parmcards4;
library(haven)
library(sqldf)
source("c:/oto/fn_tosas9x.R")
options(sqldf.dll = "d:/dll/sqlean.dll")
have<-read_sas("d:/sd1/have.sas7bdat")
print(have)
want<-sqldf("
with
  cost as ( select 'cost' as grp, date, id, cost as curval
  ,lag(cost) over (partition by id  order by id, date) as lagval
   from have),
 type as ( select 'type' as grp, date, id, type as curval
  ,lag(type) over (partition by id  order by id, date) as lagval
   from have),
 nm as ( select 'nm' as grp, date, id, nm as curval
  ,lag(nm) over (partition by id  order by id, date) as lagval
   from have)
select * from cost where curval != lagval
union all select * from type where curval != lagval
union all select * from nm   where curval != lagval
order by grp, id, date
")
want
fn_tosas9x(
      inp    = want
     ,outlib ="d:/sd1/"
     ,outdsn ="want"
     )
;;;;
%utl_rendx;

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*  R                                     | SAS                                                                           */
/*      grp       date  id curval lagval  |  ROWNAMES    GRP        DATE       ID     CURVAL    LAGVAL                    */
/*                                        |                                                                               */
/*  1  cost 2022-01-03 1A1      4      3  |      1       cost    2022-01-03    1A1     4         3                        */
/*  2  cost 2022-01-12 1A1      5      4  |      2       cost    2022-01-12    1A1     5         4                        */
/*  3  cost 2022-01-03 1A2      4      3  |      3       cost    2022-01-03    1A2     4         3                        */
/*  4    nm 2022-01-04 1A1   JUMP   BOLT  |      4       nm      2022-01-04    1A1     JUMP      BOLT                     */
/*  5    nm 2022-01-05 1A1   BOLT   JUMP  |      5       nm      2022-01-05    1A1     BOLT      JUMP                     */
/*  6    nm 2022-01-09 1A1   JUMP   BOLT  |      6       nm      2022-01-09    1A1     JUMP      BOLT                     */
/*  7    nm 2022-01-10 1A1   BOLT   JUMP  |      7       nm      2022-01-10    1A1     BOLT      JUMP                     */
/*  8    nm 2022-01-12 1A1   XTRA   BOLT  |      8       nm      2022-01-12    1A1     XTRA      BOLT                     */
/*  9    nm 2022-01-13 1A1   BOLT   XTRA  |      9       nm      2022-01-13    1A1     BOLT      XTRA                     */
/*  10   nm 2022-01-02 1A2   BOLT   XTRA  |     10       nm      2022-01-02    1A2     BOLT      XTRA                     */
/*  11   nm 2022-01-03 1A2   XTRA   BOLT  |     11       nm      2022-01-03    1A2     XTRA      BOLT                     */
/*  12   nm 2022-01-10 1A2   BOLT   XTRA  |     12       nm      2022-01-10    1A2     BOLT      XTRA                     */
/*  13 type 2022-01-06 1A1     BB     AA  |     13       type    2022-01-06    1A1     BB        AA                       */
/*  14 type 2022-01-12 1A1     CC     BB  |     14       type    2022-01-12    1A1     CC        BB                       */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
