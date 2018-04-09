# Pig Latin - Loading & Projecting Datasets

### LOADING A DATASET ###

grunt> stocks = LOAD '/user/hirw/input/stocks' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, open:float, high:float, low:float, close:float,  
volume:int, adj_close:float);  

### STRUCTURE ###

grunt> DESC stocks;

### PROJECT AND MANIPULATE FEW COLUMNS FROM DATASET ###

grunt> projection = FOREACH stocks GENERATE symbol, SUBSTRING($0, 0, 1) as sub_exch, close - open as up_or_down;

### PRINT RESULT ON SCREEN ###

grunt> DUMP projection;

### STORE RESULT IN HDFS ###

grunt> STORE projection INTO 'output/pig/simple-projection';

### LOAD 1 - WITH NO COLUMN NAMES AND DATATYPES ###

grunt> stocks = LOAD '/user/hirw/input/stocks' USING PigStorage(',');

### LOAD 2 - WITH COLUMN NAMES BUT NO DATATYPES ###

grunt> stocks = LOAD '/user/hirw/input/stocks' USING PigStorage(',') as (exchange, symbol, date, open, high, low, close, volume, adj_close);

### LOAD 3 - WITH COLUMN NAMES AND DATATYPES ###

grunt> stocks = LOAD '/user/hirw/input/stocks' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, open:float, high:float, low:float, close:float,
volume:int, adj_close:float);

### TO LOOK UP STRUCTURE OF THE RELATION ###

grunt> DESCRIBE stocks;

### WHEN COLUMN NAMES ARE NOT AVAILABLE ###

grunt> projection = FOREACH stocks GENERATE $1 as symbol, SUBSTRING($0, 0, 1) as sub_exch, $6 - $3 as up_or_down;


# Pig - Solving a Problem  

grunt> stocks = LOAD '/user/hirw/input/stocks' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, open:float, high:float, low:float, close:float,
volume:int, adj_close:float);

### FILTERING ONLY RECORDS FROM YEAR 2003 ###

filter_by_yr = FILTER stocks by GetYear(date) == 2003;

### GROUPING RECORDS BY SYMBOL ###

grunt> grp_by_sym = GROUP filter_by_yr BY symbol;

grp_by_sym: {
	group: chararray,
	filter_by_yr: {
		(exchange: chararray,symbol: chararray,date: datetime,open: float,high: float,low: float,close: float,volume: int,adj_close: float)
	}
}

### SAMPLE OUTPUT OF GROUP ###

(CASC, { (NYSE,CASC,2003-12-22T00:00:00.000Z,22.02,22.2,21.94,22.09,36700,20.29), (NYSE,CASC,2003-12-23T00:00:00.000Z,22.15,22.15,21.9,22.05,23600,20.26), ....... })
(CATO, { (NYSE,CATO,2003-10-08T00:00:00.000Z,22.48,22.5,22.01,22.06,92000,12.0), (NYSE,CATO,2003-10-09T00:00:00.000Z,21.3,21.59,21.16,21.45,373500,11.67), ....... })

### CALCULATE AVERAGE VOLUME ON THE GROUPED RECORDS ###

avg_volume = FOREACH grp_by_sym GENERATE group, ROUND(AVG(filter_by_yr.volume)) as avgvolume;

### ORDER THE RESULT IN DESCENDING ORDER ###

avg_vol_ordered = ORDER avg_volume BY avgvolume DESC;

### STORE TOP 10 RECORDS ###

top10 = LIMIT avg_vol_ordered 10;
STORE top10 INTO 'output/pig/avg-volume' USING PigStorage(',');

### EXECUTE PIG INSTRUCTIONS AS SCRIPT ###

pig /hirw-workshop/pig/scripts/average-volume.pig

### PASSING PARAMETERS TO SCRIPT ###

pig -param input=/user/hirw/input/stocks -param output=output/pig/avg-volume-params /hirw-workshop/pig/scripts/average-volume-parameters.pig

### RUNNING A PIG SCRIPT LOCALLY. INPUT AND OUTPUT LOCATION ARE POINTING TO LOCAL FILE SYSTEM ###

pig -x local -param input=/hirw-workshop/input/stocks-dataset/stocks -param output=output/stocks /hirw-workshop/pig/scripts/average-volume-parameters.pig


# Pig Latin - Joins

### LOAD stocks ###

grunt> stocks = LOAD '/user/hirw/input/stocks' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, open:float, high:float, low:float, close:float,
volume:int, adj_close:float);

### LOAD dividends ###

grunt> divs = LOAD '/user/hirw/input/dividends' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, dividends:float);
grunt> DESCRIBE divs;
divs: 
	{
		exchange: chararray,
		symbol: chararray,
		date: datetime,
		dividends: float
	}

### LOAD companies ###	

companies = LOAD '/user/hirw/input/companies' USING PigStorage(';') as (symbol:chararray, name:chararray, address: map[]);

cmp = FOREACH companies GENERATE symbol, name, address#'street', address#'city', address#'state';


### INNER JOIN ###

grunt> join_inner = JOIN stocks BY (symbol, date) , divs BY (symbol, date);	

grunt> DESCRIBE join_inner;

join_inner: {
	stocks::exchange: chararray,
	stocks::symbol: chararray,
	stocks::date: datetime,
	stocks::open: float,
	stocks::high: float,
	stocks::low: float,
	stocks::close: float,
	stocks::volume: int,
	stocks::adj_close: float,
	divs::exchange: chararray,
	divs::symbol: chararray,
	divs::date: datetime,
	divs::dividends: float
	}

grunt> join_project  = FOREACH join_inner GENERATE stocks::symbol, divs::date, divs::dividends;

grunt> DUMP join_project;

### LEFT OUTER ###

grunt> join_left = JOIN stocks BY (symbol, date) LEFT OUTER, divs BY (symbol, date);

grunt> DUMP join_left;

--Filter out records with no dividends
grunt> filterleftjoin = FILTER join_left BY divs::symbol IS NOT NULL;

grunt> DUMP filterleftjoin;


### RIGHT OUTER ###

grunt> join_right = JOIN stocks BY (symbol, date) RIGHT OUTER, divs BY (symbol, date);

grunt> DUMP join_right;


### FULL JOIN ###

--FULL OUTER will display rows from both sides matched and unmatched. Combination of LEFT OUTER and RIGHT OUTER
grunt> join_full = JOIN stocks BY (symbol, date) FULL, divs BY (symbol, date);

grunt> DUMP join_full;


### Multiway Join ###

grunt> join_multi = JOIN stocks by (symbol, date), divs by (symbol, date), cmp by symbol;

--Multiway join is only possible on inner joins and not on outer joins
grunt> join_multi = JOIN stocks by symbol, divs by symbol, cmp by symbol;

grunt> DUMP join_multi;

###CROSS###

--Takes every record in stocks and combines it with every record in divs
grunt> crs = CROSS stocks, divs;

--What about non equi joins?

grunt> non_equi = FILTER crs by stocks::symbol != divs::symbol;

grunt> limit1000 = LIMIT non_equi 1000; 


###COGROUP###

grunt> cgrp = COGROUP stocks BY (symbol, date), divs by (symbol, date);

cgrp: {
	group: (symbol: chararray,date: chararray),
	stocks: {(exchange: chararray,symbol: chararray,date: chararray,open: float,high: float,low: float,close: float,volume: int,adj_close: float)},
	divs: {(exchange: bytearray,symbol: bytearray,date: bytearray,dividends: bytearray)}
	}



((CSL,2009-05-14),{},{(ABCSE,CSL,2009-05-14,0.155)})
((CSL,2009-08-12),{(ABCSE,CSL,2009-08-12,32.65,32.73,32.17,32.54,528900,32.39)},{(ABCSE,CSL,2009-08-12,0.16)})
((CSL,2009-08-13),{(ABCSE,CSL,2009-08-13,32.58,33.19,32.49,33.15,447600,32.99)},{})

grunt> filter_empty_divs = FILTER cgrp BY (NOT IsEmpty(stocks)) AND  (NOT IsEmpty(divs));

grunt> limit10 = LIMIT filter_empty_divs 10;

grunt> DUMP limit10;


filter_empty_divs = FILTER cgrp BY (IsEmpty(group));
limit10 = LIMIT filter_empty_divs 10;
DUMP limit10;


# Wikipedia - Page Ranking

--Register the jar
REGISTER /hirw-workshop/pig/wikipedia/PageRankPig-0.0.1.jar;  
--Use PageRankLoad function with the LOAD operator  
records = LOAD '/user/ubuntu/input/pagerank' USING com.hirw.pagerankpig.PageRankLoad() AS (page:chararray, links:bag{});  
records = FILTER records BY page IS NOT NULL;  
--Calculate the no of links and points per page  
no_of_links_points_per_page = FOREACH records GENERATE page, COUNT(links) as no_of_links, (float)1/COUNT(links) as points;  
page_link_mapping = FOREACH records GENERATE page as page, FLATTEN(links) as link;  
--Remove any backlinks which does not have a page reference in the dataset  
remove_links_not_pages = JOIN no_of_links_points_per_page BY page, page_link_mapping BY link;  
only_good_pages = FOREACH remove_links_not_pages GENERATE page_link_mapping::page AS page, page_link_mapping::link as link;  
--Join links to pages and group  
joinResult = JOIN no_of_links_points_per_page BY page, only_good_pages BY page;  
groupResult = GROUP joinResult BY only_good_pages::link;  
result = FOREACH groupResult GENERATE group as page, SUM(joinResult.no_of_links_points_per_page::points) as rank;  
ordered = ORDER result BY rank desc;  
STORE ordered INTO 'output/pagerankpig';  
