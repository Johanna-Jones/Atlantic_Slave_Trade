// CALL apoc.load.xls({url}, {Name of sheet}, {config})

CALL apoc.load.xls('file:///Supplementary_tables_clean.xlsx', 'fate_2')




// FATE_2
CALL apoc.periodic.iterate('
CALL apoc.load.xls("file:///Supplementary_tables_clean.xlsx", "fate_2") yield map as row return row
','
CREATE (ft: FATE_2) SET ft = row
', {batchSize:10000, iterateList:true, parallel:true});

//iterateList:true: data returned by the first query is a list, and apoc.periodic.iterate should iterate over this list.
//parallel:true: This allows the batches to be processed in parallel, improving performance by utilizing multiple CPU cores.

// Voyage_outcome
CALL apoc.periodic.iterate('
CALL apoc.load.xls("file:///Supplementary_tables_clean.xlsx", "voyage_outcome") yield map as row return row
','
CREATE (VO: Voyage_Outcome) SET VO = row
', {batchSize:10000, iterateList:true, parallel:true});


//TON Type
CALL apoc.periodic.iterate('
CALL apoc.load.xls("file:///Supplementary_tables_clean.xlsx", "TONTYPE") yield map as row return row
','
CREATE (t: Tontype) SET t = row
', {batchSize:10000, iterateList:true, parallel:true});


//Ship Nation

CALL apoc.periodic.iterate('
CALL apoc.load.xls("file:///Supplementary_tables_clean.xlsx", "Ship_nation") yield map as row return row
','
CREATE (s: ShipNation) SET s = row
', {batchSize:10000, iterateList:true, parallel:true});

// rig
CALL apoc.periodic.iterate('
CALL apoc.load.xls("file:///Supplementary_tables_clean.xlsx", "Rig") yield map as row return row
','
CREATE (r: Rig) SET r = row
', {batchSize:10000, iterateList:true, parallel:true});


//XMIMPFLAG

CALL apoc.periodic.iterate('
CALL apoc.load.xls("file:///Supplementary_tables_clean.xlsx", "XMIMPFLAG") yield map as row return row
','
CREATE (x: XMIMPFLAG) SET x = row
', {batchSize:10000, iterateList:true, parallel:true});




CALL apoc.periodic.iterate('
CALL apoc.load.xls("file:///Supplementary_tables_clean.xlsx", "Sheet9") yield map as row 
row.Broad_region_code as Region_code,
row.Broad_region as Region,
row.Specific_region_code as Country_ColonyCode,
row.Specific_region(country_or_colony) as Country_Colony,
row.Place_code as Port_code,
row.Places_(port_or_location) as Port

// create region node
MERGE (br:Region{code:Region_code, Region: Region})
', {batchSize:10000, iterateList:true, parallel:true});


// Region node

CALL apoc.periodic.iterate('
CALL apoc.load.xls("file:///Supplementary_tables_clean.xlsx", "Sheet9") yield map as row 
RETURN
row.Broad_region_code as Region_code,
row.Broad_region as Region
',
'
// Create  Region node
MERGE (br:Region {code: Region_code, name: Region})
', 
{batchSize: 500, iterateList: true, parallel: true});


CALL apoc.periodic.iterate('
CALL apoc.load.xls("file:///Supplementary_tables_clean.xlsx", "Sheet9") yield map as row 
RETURN
row.Broad_region_code as Region_code,
row.Specific_region_code as Country_ColonyCode,
row.Specific_region_country_or_colony as Country_Colony,
row.Place_code as Port_code,
row.Places_port_or_location as Port
',
'
WITH Region_code, Country_ColonyCode, Country_Colony, Port_code, Port
// Create the inter-relationships
MERGE (br:Region{code:Region_code})
MERGE (cc:CountryColony {code: Country_ColonyCode, name: Country_Colony})
MERGE (p:Port {code: Port_code, Port:Port})
MERGE (br)-[:COUNTRY_OF]-> (cc)
MERGE (cc) -[:PORT_IN] -> (p)
', 
{batchSize: 500, iterateList: true, parallel: true});



