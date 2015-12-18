inputFile = LOAD '$INPUT' USING PigStorage('\\t')
	AS (url:chararray, pageRank:float, links:chararray)
;
previousPageRank = FOREACH inputFile
	GENERATE
		url,
		pageRank,
		TOKENIZE(links,' ') AS links:{link:tuple(url:chararray)},
		links AS strLinks
;
outLinks = FOREACH previousPageRank
	GENERATE
		pageRank/((float)COUNT(links)) AS pageRank,
		FLATTEN(links) as url
;
newPageRank = FOREACH ( COGROUP outLinks BY url, previousPageRank BY url INNER)
	GENERATE
		FLATTEN(previousPageRank.url),
		( 1 - 0.85 ) + 0.85 * SUM ( outLinks.pageRank ) AS pageRank,
		FLATTEN(previousPageRank.strLinks)
;
STORE newPageRank
	INTO '$OUTPUT'
	USING PigStorage('\t');