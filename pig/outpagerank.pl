inputFile = LOAD '$INPUT' USING PigStorage('\\t')
	AS (url:chararray, pageRank:float, links:chararray)
;
previousPageRank = FOREACH inputFile
	GENERATE
		url,
		pageRank,
		TOKENIZE(links,', ') AS links:{link:tuple(url:chararray)},
		links AS strLinks
;
outLinks = FOREACH previousPageRank
	GENERATE
		pageRank AS pageRank,
		url as url
;

outLinks = ORDER outLinks BY pageRank;

STORE outLinks
	INTO '$OUTPUT'
	USING PigStorage('\t');