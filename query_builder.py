"""
SPARQL Query Builder for Musical Works

This module builds parameterized SPARQL queries for searching musical works
in the DOREMUS Knowledge Graph with various filtering criteria.
"""

from typing import Optional, Any
import logging

logger = logging.getLogger("doremus-mcp")


def build_works_query(
    composers: Optional[list[str]] = None,
    work_type: Optional[str] = None,
    date_start: Optional[int] = None,
    date_end: Optional[int] = None,
    instruments: Optional[list[dict[str, Any]]] = None,
    place_of_composition: Optional[str] = None,
    place_of_performance: Optional[str] = None,
    duration_min: Optional[int] = None,
    duration_max: Optional[int] = None,
    topic: Optional[str] = None,
    limit: int = 50
) -> str:
    """
    Build a SPARQL query to search for musical works with various filters.
    
    Args:
        composers: List of composer names or URIs
        work_type: Genre/type keyword (sonata, symphony, concerto, etc.)
        date_start: Start year for composition date
        date_end: End year for composition date
        instruments: List of instrument specifications
        place_of_composition: Place URI or name
        place_of_performance: Place URI or name
        duration_min: Minimum duration in seconds
        duration_max: Maximum duration in seconds
        topic: Topic/subject keyword
        limit: Maximum results
        
    Returns:
        Complete SPARQL query string
    """
    
    # Standard prefixes
    prefixes = """
    PREFIX mus: <http://data.doremus.org/ontology#>
    PREFIX ecrm: <http://erlangen-crm.org/current/>
    PREFIX efrbroo: <http://erlangen-crm.org/efrbroo/>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX schema: <http://schema.org/>
    PREFIX time: <http://www.w3.org/2006/time#>
    PREFIX geonames: <http://www.geonames.org/ontology#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    """
    
    # Build SELECT clause
    select_vars = ["?expression", "SAMPLE(?title) as ?title"]
    
    if composers:
        select_vars.append("SAMPLE(?composerName) as ?composer")
    if date_start or date_end:
        select_vars.append("?start as ?compositionDate")
    if work_type:
        select_vars.append("SAMPLE(?genreLabel) as ?genre")
    if instruments:
        select_vars.append("?casting")
    if duration_min or duration_max:
        select_vars.append("?duration")
    
    select_clause = f"SELECT DISTINCT {' '.join(select_vars)}"
    
    # Build WHERE clause
    where_patterns = []
    
    # Basic expression pattern
    where_patterns.append("""
    ?expression a efrbroo:F22_Self-Contained_Expression ;
        rdfs:label ?title .
    """)
    
    # Composer filter
    if composers:
        composer_pattern = """
    ?expCreation efrbroo:R17_created ?expression ;
        ecrm:P9_consists_of ?compositionActivity .
    ?compositionActivity ecrm:P14_carried_out_by ?composer ;
        mus:U31_had_function <http://data.doremus.org/vocabulary/function/composer> .
    ?composer foaf:name ?composerName .
        """
        
        # Build composer filter
        composer_values = []
        for comp in composers:
            if comp.startswith("http://"):
                # It's a URI
                composer_values.append(f"<{comp}>")
            else:
                # It's a name - will use FILTER
                composer_pattern += f'\n    FILTER (REGEX(?composerName, "{comp}", "i"))'
        
        if composer_values:
            composer_pattern += f"\n    VALUES ?composer {{ {' '.join(composer_values)} }}"
        
        where_patterns.append(composer_pattern)
    
    # Date range filter
    if date_start or date_end:
        date_pattern = """
    ?expCreation efrbroo:R17_created ?expression ;
        ecrm:P4_has_time-span ?ts .
    ?ts time:hasEnd / time:inXSDDate ?end ;
        time:hasBeginning / time:inXSDDate ?start .
        """
        
        if date_start and date_end:
            date_pattern += f'\n    FILTER (?start >= "{date_start}"^^xsd:gYear AND ?end <= "{date_end}"^^xsd:gYear)'
        elif date_start:
            date_pattern += f'\n    FILTER (?start >= "{date_start}"^^xsd:gYear)'
        elif date_end:
            date_pattern += f'\n    FILTER (?end <= "{date_end}"^^xsd:gYear)'
        
        where_patterns.append(date_pattern)
    
    # Work type/genre filter
    if work_type:
        genre_pattern = """
    ?expression mus:U12_has_genre ?genre .
    ?genre skos:prefLabel ?genreLabel .
        """
        
        # Check if it's a full URI or keyword
        if work_type.startswith("http://"):
            genre_pattern += f"\n    VALUES ?genre {{ <{work_type}> }}"
        else:
            # Use text matching on label
            genre_pattern += f'\n    FILTER (REGEX(?genreLabel, "{work_type}", "i"))'
        
        where_patterns.append(genre_pattern)
    
    # Instrumentation filter
    if instruments:
        casting_pattern = """
    ?expression mus:U13_has_casting ?casting .
        """
        
        # For each instrument, add a casting detail pattern
        for idx, inst_spec in enumerate(instruments):
            inst_name = inst_spec.get("name", "")
            quantity = inst_spec.get("quantity")
            min_qty = inst_spec.get("min_quantity")
            max_qty = inst_spec.get("max_quantity")
            
            var_suffix = f"{idx + 1}"
            casting_pattern += f"""
    ?casting mus:U23_has_casting_detail ?castingDet{var_suffix} .
    ?castingDet{var_suffix} mus:U2_foresees_use_of_medium_of_performance ?instrument{var_suffix} .
            """
            
            # Add quantity constraints if specified
            if quantity is not None:
                casting_pattern += f"""
    ?castingDet{var_suffix} mus:U30_foresees_quantity_of_mop {quantity} .
                """
            elif min_qty is not None or max_qty is not None:
                casting_pattern += f"""
    ?castingDet{var_suffix} mus:U30_foresees_quantity_of_mop ?qty{var_suffix} .
                """
                if min_qty is not None:
                    casting_pattern += f"\n    FILTER (?qty{var_suffix} >= {min_qty})"
                if max_qty is not None:
                    casting_pattern += f"\n    FILTER (?qty{var_suffix} <= {max_qty})"
            
            # Instrument matching (name or URI)
            if inst_name.startswith("http://"):
                casting_pattern += f"""
    VALUES ?instrument{var_suffix} {{ <{inst_name}> }}
                """
            else:
                # Try to match by label using skos:exactMatch* for broader matching
                casting_pattern += f"""
    ?instrument{var_suffix} skos:prefLabel ?instLabel{var_suffix} .
    FILTER (REGEX(?instLabel{var_suffix}, "{inst_name}", "i"))
                """
        
        where_patterns.append(casting_pattern)
    
    # Duration filter
    if duration_min or duration_max:
        duration_pattern = """
    ?expression mus:U78_estimated_duration ?duration .
        """
        
        if duration_min and duration_max:
            duration_pattern += f"\n    FILTER (?duration >= {duration_min} AND ?duration <= {duration_max})"
        elif duration_min:
            duration_pattern += f"\n    FILTER (?duration >= {duration_min})"
        elif duration_max:
            duration_pattern += f"\n    FILTER (?duration <= {duration_max})"
        
        where_patterns.append(duration_pattern)
    
    # Place of composition filter
    if place_of_composition:
        place_comp_pattern = """
    ?expCreation efrbroo:R17_created ?expression ;
        ecrm:P7_took_place_at ?placeComp .
        """
        
        if place_of_composition.startswith("http://"):
            place_comp_pattern += f"\n    VALUES ?placeComp {{ <{place_of_composition}> }}"
        else:
            place_comp_pattern += f"""
    ?placeComp rdfs:label ?placeCompLabel .
    FILTER (REGEX(?placeCompLabel, "{place_of_composition}", "i"))
            """
        
        where_patterns.append(place_comp_pattern)
    
    # Place of performance filter
    if place_of_performance:
        place_perf_pattern = """
    ?performance a efrbroo:F31_Performance ;
        efrbroo:R25_performed / ecrm:P165_incorporates ?expression ;
        ecrm:P7_took_place_at ?placePerf .
        """
        
        if place_of_performance.startswith("http://"):
            place_perf_pattern += f"\n    VALUES ?placePerf {{ <{place_of_performance}> }}"
        else:
            place_perf_pattern += f"""
    ?placePerf rdfs:label ?placePerfLabel .
    FILTER (REGEX(?placePerfLabel, "{place_of_performance}", "i"))
            """
        
        where_patterns.append(place_perf_pattern)
    
    # Topic filter (using text matching on labels and comments)
    if topic:
        topic_pattern = f"""
    {{
        ?expression rdfs:comment ?comment .
        FILTER (REGEX(?comment, "{topic}", "i"))
    }} UNION {{
        ?expression rdfs:label ?topicLabel .
        FILTER (REGEX(?topicLabel, "{topic}", "i"))
    }}
        """
        where_patterns.append(topic_pattern)
    
    # Combine WHERE patterns
    where_clause = "WHERE {\n" + "\n".join(where_patterns) + "\n}"
    
    # GROUP BY clause
    group_vars = ["?expression"]
    if instruments:
        group_vars.append("?casting")
    if date_start or date_end:
        group_vars.append("?start")
    if duration_min or duration_max:
        group_vars.append("?duration")
    
    group_clause = f"GROUP BY {' '.join(group_vars)}" if len(group_vars) > 1 else ""
    
    # ORDER BY clause
    order_clause = ""
    if date_start or date_end:
        order_clause = "ORDER BY ?start"
    
    # Build complete query
    query = f"""
    {prefixes}
    
    {select_clause}
    {where_clause}
    {group_clause}
    {order_clause}
    LIMIT {limit}
    """
    
    logger.debug(f"Built query with filters: composers={composers}, type={work_type}, dates={date_start}-{date_end}")
    
    return query


def get_instrument_uris() -> dict[str, list[str]]:
    """
    Get a mapping of common instrument names to their URIs in the knowledge graph.
    
    Returns:
        Dictionary mapping instrument names to lists of possible URIs
    """
    
    return {
        "violin": [
            "http://data.doremus.org/vocabulary/iaml/mop/svl",
            "http://www.mimo-db.eu/InstrumentsKeywords/3573"
        ],
        "viola": [
            "http://data.doremus.org/vocabulary/iaml/mop/sva",
            "http://www.mimo-db.eu/InstrumentsKeywords/3561"
        ],
        "cello": [
            "http://data.doremus.org/vocabulary/iaml/mop/svc",
            "http://www.mimo-db.eu/InstrumentsKeywords/3582"
        ],
        "piano": [
            "http://data.doremus.org/vocabulary/iaml/mop/kpf",
            "http://www.mimo-db.eu/InstrumentsKeywords/2299"
        ],
        "flute": [
            "http://data.doremus.org/vocabulary/iaml/mop/wfl",
            "http://www.mimo-db.eu/InstrumentsKeywords/3955"
        ],
        "clarinet": [
            "http://data.doremus.org/vocabulary/iaml/mop/wcl",
            "http://www.mimo-db.eu/InstrumentsKeywords/3836"
        ],
        "oboe": [
            "http://data.doremus.org/vocabulary/iaml/mop/wob",
            "http://www.mimo-db.eu/InstrumentsKeywords/4164"
        ],
        "bassoon": [
            "http://data.doremus.org/vocabulary/iaml/mop/wba",
            "http://www.mimo-db.eu/InstrumentsKeywords/3795"
        ],
        "horn": [
            "http://data.doremus.org/vocabulary/iaml/mop/bhn"
        ],
        "trumpet": [
            "http://data.doremus.org/vocabulary/iaml/mop/btp"
        ],
        "orchestra": [
            "http://data.doremus.org/vocabulary/iaml/mop/o"
        ],
        "choir": [
            "http://data.doremus.org/vocabulary/iaml/mop/c"
        ],
        "strings": [
            "http://data.doremus.org/vocabulary/iaml/mop/s"
        ],
        "voice": [
            "http://data.doremus.org/vocabulary/iaml/mop/v"
        ]
    }


def get_genre_uris() -> dict[str, str]:
    """
    Get a mapping of common genre names to their URIs in the knowledge graph.
    
    Returns:
        Dictionary mapping genre names to URIs
    """
    
    return {
        "symphony": "http://data.doremus.org/vocabulary/iaml/genre/sy",
        "sonata": "http://data.doremus.org/vocabulary/iaml/genre/sn",
        "concerto": "http://data.doremus.org/vocabulary/iaml/genre/co",
        "opera": "http://data.doremus.org/vocabulary/iaml/genre/op",
        "quartet": "http://data.doremus.org/vocabulary/iaml/genre/qt",
        "trio": "http://data.doremus.org/vocabulary/iaml/genre/tr",
        "melody": "http://data.doremus.org/vocabulary/iaml/genre/mld",
        "mass": "http://data.doremus.org/vocabulary/iaml/genre/ms",
        "overture": "http://data.doremus.org/vocabulary/iaml/genre/ov"
    }
