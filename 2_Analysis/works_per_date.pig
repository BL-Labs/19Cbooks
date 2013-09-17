
raw = LOAD 'c19/metadatalist' 
        AS (id:chararray, title:chararray, firstauthor: chararray, place:chararray, publisher:chararray, pubdate:chararray);

dates = FOREACH raw GENERATE id as id, pubdate as pubdate;

-- Works per date (May or may not correspond to a year due to cataloguing)

date_group = GROUP dates BY pubdate;

works_per_date = FOREACH date_group GENERATE group as pubdate, COUNT(dates);

ordered_works_per_date = ORDER works_per_date BY pubdate;

STORE ordered_works_per_date INTO 'c19/works_per_date';

