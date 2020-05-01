/*create a local directory (perm)to save files and results*/
libname perm 'C:\Users\51950005\Desktop\SAS\ERA2\Replication assignment';

/*remote login to SAS cloud*/
%let wrds = wrds.wharton.upenn.edu 4016;
options comamid=TCP remote=WRDS;
signon username=_prompt_;

/*create a remote work folder (rwork) to work on files in SAS cloud*/
Libname rwork slibref=work server=wrds;


/*download requisite data from wrds*/
rsubmit;
proc download data=comp.security out=perm.comp_security; /*comp.security -> security identifier data from compustat*/
run;
proc download data=comp.idxcst_his out=perm.comp_idxcst_his; /*comp.idxcst_his -> index constitutent data from compustat*/
run;
proc download data=crsp.ccmxpf_linktable out=perm.crsp_ccmxpf_linktable; /*crsp.ccmxpf_linktable -> compustat (gvkey) and crsp (lpermno) link table*/
run;
endrsubmit;


/*set the date range for study*/
%let bdate=01jan1974;        /*start calendar date of fiscal period end*/
%let edate=31dec1981;        /*end calendar date of fiscal period end  */


/*create a file Gvkeys which contains the security identifiers from compustat and crsp and also has the link start date and end date*/
rsubmit;
proc sql; create table gvkeys
  as select distinct a.gvkey, b.lpermco as permco, b.lpermno as permno,
  coalesce (b.linkenddt,'31dec9999'd) as linkenddt format date9.,
  coalesce (c.ibtic) as ticker, b.linkdt format date9.
  from comp.idxcst_his (where=(gvkeyx='000003')) a  /*gvkeyx=000003 refers to S&P 500*/
  left join crsp.ccmxpf_linktable
           (where=(usedflag=1 and linkprim in ('P','C'))) b
  on a.gvkey=b.gvkey
  left join comp.security c
  on a.gvkey=c.gvkey
 order by gvkey, linkdt, ticker;
quit;
endrsubmit;

rsubmit;
data gvkeys; set gvkeys;
  by gvkey linkdt ticker;
  if last.linkdt;
run;
endrsubmit;

rsubmit;
proc download data=gvkeys out=perm.gvkeys;
run;
endrsubmit; 

/* Extracting Compustat Data (fundamental quarterly)  and merging it with Gvkeys file */
rsubmit;
proc sql;
  create table comp2
  (keep=gvkey fyearq fqtr conm datadate rdq epsfxq epspxq
        prccq ajexq spiq cshoq prccq ajexq spiq cshoq mcap /*Compustat variables*/
        cshprq cshfdq rdq saleq atq fyr datafqtr          
        permno ticker )     
as select *, (a.cshoq*a.prccq) as mcap
from comp.fundq
    (where=((not missing(saleq) or atq>0) and consol='C' and
    popsrc='D' and indfmt='INDL' and datafmt='STD' and not missing(datafqtr))) a
    inner join
    (select distinct gvkey, permno, ticker, min(linkdt) as mindate,
    max(linkenddt) as maxdate from gvkeys group by gvkey, ticker) b
    on a.gvkey=b.gvkey and b.mindate<=a.datadate<=b.maxdate;
quit;
endrsubmit;

rsubmit;
proc sort data=comp2 nodupkey; by gvkey fqtr fyearq;run;
endrsubmit;

rsubmit;
proc download data=comp2 out=perm.comp2;
run;
endrsubmit;


/*Calculate SUE using the random walk model*/
rsubmit;
data sue1/view=sue1; set comp2;
 by gvkey fqtr fyearq;
    if dif(fyearq)=1 then do;
      lagadj=lag(ajexq); lageps_p=lag(epspxq);lageps_d=lag(epsfxq);
      lagshr_p=lag(cshprq);lagshr_d=lag(cshfdq);lagspiq=lag(spiq);
    end;
    if first.gvkey then do;
    lageps_d=.;lagadj=.; lageps_p=.;
    lagshr_p=.;lagshr_d=.;lagspiq=.;
    end;

    if basis='P' then do;
       actual1=epspxq/ajexq; expected1=lageps_p/lagadj;
       end;
    else if basis='D' then do;
        actual1=epsfxq/ajexq; expected1=lageps_d/lagadj;
        end;
    else do;
        actual1=epspxq/ajexq; expected1=lageps_p/lagadj;
        end;

    sue1=(actual1-expected1)/(prccq/ajexq);

    format sue1 percent7.4 rdq date9.;
  label datadate='Calendar date of fiscal period end';
  keep ticker permno gvkey conm fyearq fqtr fyr datadate
       rdq sue1 basis
       act prccq mcap;
run;
endrsubmit;

rsubmit;
proc download data=sue1 out=perm.sue1; run; endrsubmit;


proc print data=perm.comp2 (obs = 25); run;


/*For reporting date quarter (rdq), get the equivalent nearest date from CRSP*/
rsubmit;
proc sql;
  create view eads1
     as select a.*, b.date as rdq1 format=date9.
     from (select distinct rdq from comp2) a
     left join (select distinct date from crsp.dsf) b
     on 5>=b.date-a.rdq>=0
     group by rdq
     having b.date-a.rdq=min(b.date-a.rdq);
quit;
endrsubmit;

rsubmit; proc print data=perm.eads1 (obs=25); where rdq neq rdq1; run; endrsubmit;

rsubmit; proc print data=eads1 (obs=25); run; endrsubmit;

/*update the SUE table with the nearest date to reporting date information from CRSP*/
rsubmit;
proc sql;
create table sue_final
     as select a.*, b.rdq1
     label='Adjusted Report Date of Quarterly Earnings'
     from sue1 a left join eads1 b
     on a.rdq=b.rdq
     order by a.gvkey, a.fyearq desc, a.fqtr desc;
quit;
endrsubmit;

rsubmit; proc print data=sue_final (obs=25); run;

rsubmit; proc download data=sue_final out=perm.sue_final; run; endrsubmit;


/*additional filters applied to sue-final table*/
rsubmit;
data sue_final1;
   retain gvkey ticker permno conm fyearq fqtr datadate fyr rdq rdq1 leadrdq1
           mcap act sue1;
   set sue_final; 
   by gvkey descending fyearq descending fqtr;
   leadrdq1=lag(rdq1); /*the next consecutive EAD*/
   format rdq1 leadrdq1 date9.; 
   if first.gvkey then leadrdq1=intnx('month',rdq1,3,'sameday'); 
   if leadrdq1=rdq1 then delete;   
   if (nmiss(sue1)=0  
   or (not missing(repdats) and abs(intck('day',repdats,rdq))<=1));
   if (not missing(rdq) and prccq>1 and mcap>5.0);
   keep gvkey ticker permno conm fyearq fqtr datadate fyr rdq rdq1 leadrdq1
          mcap act basis sue1;
   label
      leadrdq1='Lead Adjusted Report Date of Quarterly Earnings'
      basis='Primary/Diluted Basis'
      act='Actual Reported Earnings per Share'
      sue1='Earnings Surprise (Seasonal Random Walk)'
run; 
endrsubmit;

rsubmit; proc print data=sue_final1 (obs=25); run; endrsubmit;

rsubmit; proc download data=sue_final1 out=perm.sue_final1; run; endrsubmit;



%let bdate=1974Jan01;        /*start calendar date of fiscal period end*/
%let edate=1981Dec31;        /*end calendar date of fiscal period end  */

rsubmit;
proc sql; 
   create table crsprets 
   as select a.permno, a.prc, a.date, abs(a.prc*a.shrout) as mcap,
             b.rdq1, b.leadrdq1, b.sue1,a.ret,
             c.vwretd as mkt, (a.ret-c.vwretd) as exret
   from crsp.dsf (where =('01Jan1974'd<=date<='31Dec1981'd)) a inner join
   sue_final1 (where=(nmiss(rdq, leadrdq1, permno)=0 and leadrdq1-rdq1>30)) b 
   on a.permno=b.permno and b.rdq1-5<=a.date<=b.leadrdq1+5
   left join crsp.dsi (keep=date vwretd) c
   on a.date=c.date
   order by a.permno, b.rdq1, a.date;
quit; 
endrsubmit;

rsubmit; proc print data=crsprets (obs=50); run; endrsubmit;

rsubmit; proc download data=crsprets out=perm.crsprets; run; endrsubmit;

 
/*create a count variable to count each day w.r.t. rdq ranging from 1,2,... 60,, upto next rdq*/
rsubmit;
data temp/view=temp; set crsprets;
  by permno rdq1 date;
  lagmcap=lag(mcap);
  if first.permno then lagmcap=.;
  if date=rdq1 then count=0;
  else if date>rdq1 then count+1;
  format date date9. exret percent7.4;
  if rdq1<=date<=leadrdq1;
run;
endrsubmit;

rsubmit; proc print data=temp (obs=50); run; endrsubmit;

rsubmit; proc download data=temp out=perm.temp; run; endrsubmit;


/*Create 10 ranks, and re assign the ranks on each count (i.e. new day)*/
rsubmit;
proc sort data=temp out=peadrets nodupkey; by count permno rdq1;run;
proc rank data=peadrets out=peadrets groups=10;
  by count; var sue1;
  ranks sue1r;
run;
endrsubmit;

rsubmit; proc download data=peadrets out=perm.peadrets; run; endrsubmit;
 
/*form portfolios based on SUEs*/
rsubmit;
proc sort data=peadrets (where=(not missing(sue1))) out=peadsue1; by count sue1r;run; endrsubmit;

/*calculate market cap weighted average return for each sue-rank (sue1r) on each day (count) */ 
rsubmit;
proc means data=peadsue1 noprint;
  by count sue1r;
  var exret; weight lagmcap;
  output out=peadsue1port mean=/autoname;
run; 
proc transpose data=peadsue1port out=peadsue1port;
  by count; id sue1r;
  var exret_mean;
run;
/*label the portfolios (lowest rank as the most negative return portfolio)*/ 
data peadsue1port; set peadsue1port ;
   if count=0 then do;
  _0=0;_1=0;_2=0;_3=0;_4=0;_5=0;_6=0;_7=0;_8=0;_9=0;end;
  label
  _0='Rets of Most negative SUE port' _1='Rets of SUE Portfolio #2'
  _2='Rets of SUE Portfolio #3'   _3='Rets of SUE Portfolio #4'
  _4='Rets of SUE Portfolio #5'   _5='Rets of SUE Portfolio #6'
  _6='Rets of SUE Portfolio #7'   _7='Rets of SUE Portfolio #8'
  _8='Rets of SUE Portfolio #9'   
  _9='Rets of most positive SUE port';
   drop _name_;
run;
endrsubmit;

rsubmit; proc download data=peadsue1port out=perm.peadsuefinalport; run; endrsubmit;


/*Cumulate daily Returns for upto 60 days for each portfolio*/
rsubmit;
proc expand data=peadsue1port out=peadsue1port;
  id count; where count<=60;
  convert _0=sueport1/transformout=(sum);
  convert _1=sueport2/transformout=(sum);
  convert _2=sueport3/transformout=(sum );
  convert _3=sueport4/transformout=(sum);
  convert _4=sueport5/transformout=(sum);
  convert _5=sueport6/transformout=(sum);
  convert _6=sueport7/transformout=(sum );
  convert _7=sueport8/transformout=(sum);
  convert _8=sueport9/transformout=(sum);
  convert _9=sueport10/transformout=(sum);
quit;
endrsubmit;

rsubmit; proc download data=peadsue1port out=perm.peadsue60dayport; run; endrsubmit;


/*plot the PEAD graph*/ 
rsubmit;
options nodate orientation=landscape;
ods pdf file="PEAD_sue1.pdf";
goptions device=pdfc; 
axis1 label=(angle=90 "Cumulative Value-Weighted Excess Returns");
axis2 label=("Event time, t=0 is Earnings Announcement Date");
symbol interpol=join w=4 l=1;
proc gplot data =peadsue1port;
 Title 'CARs following EAD for Analyst-based SUE portfolios';
 Title2 'Sample: S&P 500 members, Period: 1980-2011';
 plot (sueport1 sueport2 sueport3 sueport4 sueport5 sueport6 sueport7 sueport8 sueport9 sueport10)*count
  /overlay legend vaxis=axis1 haxis=axis2;
run;quit;
ods pdf close;
endrsubmit;

