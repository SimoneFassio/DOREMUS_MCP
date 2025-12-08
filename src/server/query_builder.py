from typing import Optional, List, Dict, Any, Union
import logging
from src.server.query_container import QueryContainer

logger = logging.getLogger("doremus-mcp")

# Basic nationality mapping
COUNTRY_CODES = {
    "german": "DE", "germany": "DE",
    "french": "FR", "france": "FR",
    "italian": "IT", "italy": "IT",
    "english": "GB", "uk": "GB", "united kingdom": "GB",
    "american": "US", "usa": "US", "united states": "US",
    "austrian": "AT", "austria": "AT",
    "russian": "RU", "russia": "RU",
    # TODO Add more
}

def _resolve_entity(name: str, entity_type: str) -> Optional[str]:
    """
    Helper to resolve a name to a URI using internal tools.
    Returns the first matching URI or None.
    """
    try:
        from tools_internal import find_candidate_entities_internal

        # Use "others" for generic or specific types not in [artist, vocabulary]
        # But 'find_candidate_entities_internal' supports: artist, vocabulary, others.
        # For Genre/Key we might want 'vocabulary' or specific class?
        # The tool says: artist, vocabulary, others.
        
        search_type = "others"
        if entity_type == "composer":
            search_type = "artist"
        elif entity_type in ["genre", "key"]:
            search_type = "vocabulary"
            
        result = find_candidate_entities_internal(name, search_type)

        if result.get("matches_found", 0) > 0:
            # Take the first one (most relevant)
            first_entity = result["entities"][0]
            return first_entity.get("entity") # URI
            
    except Exception as e:
        logger.warning(f"Failed to resolve entity {name}: {e}")
    
    return None

def query_works(
    query_id: str,
    title: Optional[str] = None,
    composer_name: Optional[str] = None,
    composer_nationality: Optional[str] = None,
    genre: Optional[str] = None,
    place_of_composition: Optional[str] = None,
    musical_key: Optional[str] = None,
    limit: int = 50
) -> QueryContainer:
    """
    Initialize a QueryContainer with a baseline query for Musical Works.
    """
    qc = QueryContainer(query_id)
    qc.set_limit(limit)
    
    # 1. Define Select variables
    # We want: ?expression, ?title (SAMPLE), ?composer (SAMPLE)
    # User example: SELECT DISTINCT ?expression SAMPLE(?title) as ?title
    # We will use the user's preferred robust pattern.
    
    select_vars = ["?expression", "SAMPLE(?title) as ?title"]
    if composer_name or composer_nationality:
        select_vars.append("SAMPLE(?composerName) as ?composer")
        
    qc.set_select(select_vars)
    
    # 2. Variable Resolvers
    resolved_composer = _resolve_entity(composer_name, "composer") if composer_name else None
    resolved_genre = _resolve_entity(genre, "genre") if genre else None
    resolved_place = _resolve_entity(place_of_composition, "others") if place_of_composition else None
    resolved_key = _resolve_entity(musical_key, "key") if musical_key else None

    # 3. Core Module: Expression & Title
    # Use simple label for display as requested
    core_module = {
        "id": "work_core",
        "type": "pattern",
        "triples": [
            "?expression a efrbroo:F22_Self-Contained_Expression ;",
            "    rdfs:label ?title ."
        ],
        "defined_vars": ["?expression", "?title"]
    }
    qc.add_module(core_module)
    
    # 4. Filter Modules
    
    # Title Filter
    if title:
        title_filter_module = {
            "id": "work_title_filter",
            "type": "filter",
            "triples": [
                f'FILTER (REGEX(?title, "{title}", "i"))'
            ],
            "required_vars": ["?expression"]
        }
        qc.add_module(title_filter_module)
    
    # Composer Filter
    if composer_name or composer_nationality:
        # Structure: ?expCreation -> ?expression
        #            ?expCreation -> ?compositionActivity (consists_of)
        #            ?compositionActivity -> ?composer (carried_out_by)
        
        triples = [
            "?expCreation efrbroo:R17_created ?expression ;",
            "    ecrm:P9_consists_of ?compositionActivity .",
            "?compositionActivity ecrm:P14_carried_out_by ?composer ;",
            "    mus:U31_had_function <http://data.doremus.org/vocabulary/function/composer> .",
            "?composer foaf:name ?composerName ."
        ]
        
        if resolved_composer:
            # Add VALUES clause with comment
            triples.append((f"VALUES ?composer {{ <{resolved_composer}> }}", f"{composer_name}"))
        elif composer_name:
            # Fallback to regex if resolution failed? 
            # User explicitly said "take the first URI... then add the URI to the query".
            # If resolution fails, maybe we should still try regex or just fail?
            # I'll add regex fallback for robustness.
            triples.append(f'FILTER (REGEX(?composerName, "{composer_name}", "i"))')

        if composer_nationality:
            code = COUNTRY_CODES.get(composer_nationality.lower())
            if code:
                triples.append(f'?composer schema:birthPlace / geonames:countryCode "{code}" .')
            else:
                # Fallback if unknown code?
                logger.warning(f"Unknown nationality code for: {composer_nationality}")
        
        composer_module = {
            "id": "work_composer_filter",
            "type": "filter",
            "triples": triples,
            "required_vars": ["?expression"],
            "defined_vars": ["?expCreation", "?compositionActivity", "?composer", "?composerName"]
        }
        qc.add_module(composer_module)

    # Genre Filter
    if genre:
        triples = [
             "?expression mus:U12_has_genre ?genre .",
        ]
        
        if resolved_genre:
             triples.append((f"VALUES ?genre {{ <{resolved_genre}> }}", f"{genre}"))
        else:
             # Fallback
             triples.append("?genre skos:prefLabel ?genreLabel .")
             triples.append(f'FILTER (REGEX(?genreLabel, "{genre}", "i"))')
             
        genre_module = {
            "id": "work_genre_filter",
            "type": "filter",
            "triples": triples,
            "required_vars": ["?expression"],
            "defined_vars": ["?genre"]
        }
        qc.add_module(genre_module)
        
    # Place of Composition
    if place_of_composition:
        # Pattern:
        # ?expCreation ecrm:P7_took_place_at ?placeComp .
        
        triples = [
            # Ensure ?expCreation is bound. If composer filter wasn't added, we need to bind ?expCreation to ?expression
        ]
        
        # Check if we need to link expCreation to expression again or if it's already there?
        # QueryContainer doesn't automatically deduplicate patterns (yet), 
        # so we must be careful. Ideally, we define a "Creation Event" module if needed.
        # But for now, we can just re-state or use OPTIONAL/UNION if we were advanced.
        # Safest is to restate the link:
        triples.append("?expCreation efrbroo:R17_created ?expression .")
        triples.append("?expCreation ecrm:P7_took_place_at ?placeComp .")
        
        if resolved_place:
             triples.append((f"VALUES ?placeComp {{ <{resolved_place}> }}", f"{place_of_composition}"))
        else:
             triples.append("?placeComp rdfs:label ?placeCompLabel .")
             triples.append(f'FILTER (REGEX(?placeCompLabel, "{place_of_composition}", "i"))')

        place_module = {
            "id": "work_place_filter",
            "type": "filter",
            "triples": triples,
            "required_vars": ["?expression"],
            "defined_vars": ["?expCreation", "?placeComp"]
        }
        qc.add_module(place_module)

    # Key Filter
    if musical_key:
        triples = [
            "?expression mus:U11_has_key ?key ."
        ]
        if resolved_key:
             triples.append((f"VALUES ?key {{ <{resolved_key}> }}", f"{musical_key}"))
        else:
             triples.append("?key skos:prefLabel ?keyLabel .")
             triples.append(f'FILTER (REGEX(?keyLabel, "{musical_key}", "i"))')
             
        key_module = {
            "id": "work_key_filter",
            "type": "filter",
            "triples": triples,
            "required_vars": ["?expression"],
            "defined_vars": ["?key"]
        }
        qc.add_module(key_module)
        
    return qc

def query_performance(
    query_id: str,
    title: Optional[str] = None,
    location: Optional[str] = None,
    carried_out_by: Optional[List[str]] = None,
    limit: int = 50
) -> QueryContainer:
    """
    Initialize a QueryContainer with a baseline query for Performances.
    """
    qc = QueryContainer(query_id)
    qc.set_limit(limit)
    
    # Select variables
    qc.set_select(["?performance", "SAMPLE(?title) as ?title", "SAMPLE(?locationName) as ?locationName"])
    
    # Core Module: Performance Entity
    core_module = {
        "id": "performance_core",
        "type": "pattern",
        "triples": [
            "?performance a efrbroo:F31_Performance ;",
            "    rdfs:label ?title ."
        ],
        "defined_vars": ["?performance", "?title"]
    }
    qc.add_module(core_module)
    
    # Title Filter (Advanced)
    if title:
        title_filter_module = {
            "id": "performance_title_filter",
            "type": "filter",
            "triples": [
                f'FILTER (REGEX(?title, "{title}", "i"))'
            ],
            "required_vars": ["?title"]
        }
        qc.add_module(title_filter_module)

    
    # Location Filter
    if location:
        triples = [
            "?performance ecrm:P7_took_place_at ?place ."
        ]
        
        triples.append("?place rdfs:label ?locationName .")
        triples.append(f'FILTER (REGEX(?locationName, "{location}", "i"))')
        
        loc_module = {
            "id": "performance_location",
            "type": "filter",
            "triples": triples,
            "required_vars": ["?performance"],
            "defined_vars": ["?place", "?locationName"]
        }
        qc.add_module(loc_module)
    else:
         # Optional location for SELECT
         loc_opt_module = {
            "id": "performance_location_optional",
            "type": "optional",
            "triples": [
                "OPTIONAL { ?performance ecrm:P7_took_place_at ?place . ?place rdfs:label ?locationName }"
            ],
            "defined_vars": ["?locationName"]
         }
         qc.add_module(loc_opt_module)

    # Performer Filter (carried_out_by)
    if carried_out_by:
        for idx, person_name in enumerate(carried_out_by):
            # Resolve if possible
            resolved_uri = _resolve_entity(person_name, "artist")
            
            # The pattern is recursive/deep: ?performance -> consists_of* -> activity -> carried_out_by -> artist
            # We use a path that covers both conductors and musicians
            
            triples = []
            var_suffix = f"_{idx}"
            artist_var = f"?artist{var_suffix}"
            name_var = f"?artistName{var_suffix}"
            
            # Use property path for flexibility
            # ecrm:P9_consists_of* means O or more, effectively finding sub-activities
            triples.append(f"?performance ecrm:P9_consists_of+ ?activity{var_suffix} .")
            triples.append(f"?activity{var_suffix} ecrm:P14_carried_out_by {artist_var} .")
            
            if resolved_uri:
                triples.append((f"VALUES {artist_var} {{ <{resolved_uri}> }}", f"{person_name}"))
            else:
                 triples.append(f"{artist_var} foaf:name {name_var} .")
                 triples.append(f'FILTER (REGEX({name_var}, "{person_name}", "i"))')

            perf_module = {
                "id": f"performance_artist_{idx}",
                "type": "filter",
                "triples": triples,
                "required_vars": ["?performance"]
            }
            qc.add_module(perf_module)
        
    return qc

def query_artist(
    query_id: str,
    name: Optional[str] = None,
    nationality: Optional[str] = None,
    birth_place: Optional[str] = None,
    death_place: Optional[str] = None,
    work_name: Optional[str] = None,
    limit: int = 50
) -> QueryContainer:
    """
    Initialize a QueryContainer with a baseline query for Artists.
    """
    qc = QueryContainer(query_id)
    qc.set_limit(limit)
    
    # Select variables
    qc.set_select(["?artist", "SAMPLE(?name) as ?name"])
    
    # Core Module: Artist Entity
    core_module = {
        "id": "artist_core",
        "type": "pattern",
        "triples": [
            "?artist a ecrm:E21_Person ;",
            "    rdfs:label ?name ."
        ],
        "defined_vars": ["?artist", "?name"]
    }
    qc.add_module(core_module)
    
    # Name Filter
    if name:
        resolved_artist = _resolve_entity(name, "composer")
        triples = []
        if resolved_artist:
            triples.append((f"VALUES ?artist {{ <{resolved_artist}> }}", f"{name}"))
        else:
            triples.append(f'FILTER (REGEX(?name, "{name}", "i"))')
        
        name_module = {
            "id": "artist_name_filter",
            "type": "filter",
            "triples": triples,
            "required_vars": ["?artist"]
        }
        qc.add_module(name_module)

    # Nationality Filter
    if nationality:
         country_code = COUNTRY_CODES.get(nationality.lower())
         triples = []
         if country_code:
             triples.append(f'?artist schema:birthPlace / geonames:countryCode "{country_code}" .')
         else:
             logger.warning(f"Unknown nationality code for: {nationality}")
             pass
         
         if triples:
             nat_module = {
                "id": "artist_nationality_filter",
                "type": "filter",
                "triples": triples,
                "required_vars": ["?artist"]
             }
             qc.add_module(nat_module)

    # Birth Place: ?artist schema:birthPlace ?bp . ?bp rdfs:label ?bpLabel
    if birth_place:
        triples = []
        triples.append("?artist schema:birthPlace ?bp .")
        triples.append("?bp rdfs:label ?bpLabel .")
        triples.append(f'FILTER (REGEX(?bpLabel, "{birth_place}", "i"))')
        
        bp_module = {
            "id": "artist_birth_place_filter",
            "type": "filter",
            "triples": triples,
            "required_vars": ["?artist"]
        }
        qc.add_module(bp_module)

    # Death Place: ?artist schema:deathPlace ?dp . ?dp rdfs:label ?dpLabel
    if death_place:
        triples = []
        triples.append("?artist schema:deathPlace ?dp .")
        triples.append("?dp rdfs:label ?dpLabel .")
        triples.append(f'FILTER (REGEX(?dpLabel, "{death_place}", "i"))')
        
        dp_module = {
            "id": "artist_death_place_filter",
            "type": "filter",
            "triples": triples,
            "required_vars": ["?artist"]
        }
        qc.add_module(dp_module)

    # Work Name Filter
    if work_name:
        triples = []
        triples.append("?performanceWork ecrm:P9_consists_of / ecrm:P14_carried_out_by ?artist .")
        triples.append("?performanceWork efrbroo:R17_created ?expression .")
        triples.append("?expression rdfs:label ?workTitle .")
        triples.append(f'FILTER (REGEX(?workTitle, "{work_name}", "i"))')
        
        work_module = {
            "id": "artist_work_filter",
            "type": "filter",
            "triples": triples,
            "required_vars": ["?artist"]
        }
        qc.add_module(work_module)

    return qc
