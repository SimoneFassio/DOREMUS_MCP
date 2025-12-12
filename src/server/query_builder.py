from typing import Optional, List, Dict, Any, Union
import logging
from src.server.query_container import QueryContainer, create_triple_element, create_select_element
from src.server.utils import find_candidate_entities_utils
from fastmcp import Context

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
    "norwegian": "NO", "norway": "NO",
    "swedish": "SE", "sweden": "SE",
    "spanish": "ES", "spain": "ES",
}

def _resolve_entity(name: str, entity_type: str) -> Optional[str]:
    """
    Helper to resolve a name to a URI using internal tools.
    Returns the first matching URI or None.
    """
    try:
        # Use "others" for generic or specific types not in [artist, vocabulary]
        # But 'find_candidate_entities_internal' supports: artist, vocabulary, others.
        # For Genre/Key we might want 'vocabulary' or specific class?
        # The tool says: artist, vocabulary, others.
        
        search_type = "others"
        if entity_type == "composer":
            search_type = "artist"
        elif entity_type in ["genre", "key"]:
            search_type = "vocabulary"
            
        result = find_candidate_entities_utils(name, search_type)

        if result.get("matches_found", 0) > 0:
            # Take the first one (most relevant)
            first_entity = result["entities"][0]
            return first_entity.get("entity") # URI
            
    except Exception as e:
        logger.warning(f"Failed to resolve entity {name}: {e}")
    
    return None

async def query_works(
    query_id: str,
    question: str = "",
    title: Optional[str] = None,
    composer_name: Optional[str] = None,
    composer_nationality: Optional[str] = None,
    genre: Optional[str] = None,
    place_of_composition: Optional[str] = None,
    musical_key: Optional[str] = None,
    limit: int = 50, 
) -> QueryContainer:
    """
    Initialize a QueryContainer with a baseline query for Musical Works.
    """
    qc = QueryContainer(query_id, question)
    qc.set_limit(limit)
    
    # 1. Define Select variables
    # We want: ?expression, ?title (SAMPLE), ?composer (SAMPLE)
    # User example: SELECT DISTINCT ?expression SAMPLE(?title) as ?title
    # We will use the user's preferred robust pattern.
    
    select_vars = [create_select_element("expression", "efrbroo:F22_Self-Contained_Expression", False), create_select_element("title", "", True)]
    if composer_name or composer_nationality:
        select_vars.append(create_select_element("composer", "ecrm:E21_Person", True))
        
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
        "type": "where",
        "scope": "main",
        "triples": [
            {"subj": create_triple_element("expression", "efrbroo:F22_Self-Contained_Expression", "var"), 
             "pred": create_triple_element("a", "a", "uri"),
             "obj": create_triple_element("expression_type", "efrbroo:F22_Self-Contained_Expression", "uri")},
            {"subj": create_triple_element("expression", "efrbroo:F22_Self-Contained_Expression", "var"),
             "pred": create_triple_element("rdfsLabel", "rdfs:label", "uri"),
             "obj": create_triple_element("title", "", "var")}
        ]
    }
    await qc.add_module(core_module)
    
    # 4. Filter Modules
    
    # Title Filter
    if title:
        title_filter_module = {
            "id": "work_title_filter",
            "type": "filter",
            "scope": "main",
            "filter_st": [
                {'function': 'REGEX', 'args': ['?title', f"\'{title}\'", "\'i\'"]}
            ],
        }
        await qc.add_module(title_filter_module)
    
    # Composer Filter
    if composer_name or composer_nationality:
        # Structure: ?expCreation -> ?expression
        #            ?expCreation -> ?compositionActivity (consists_of)
        #            ?compositionActivity -> ?composer (carried_out_by)
        
        triples = [
            {
            "subj": create_triple_element("expCreation", "efrbroo:F28_Expression_Creation", "var"),
            "pred": create_triple_element("efrbroo:R17_created", "efrbroo:R17_created", "uri"),
            "obj": create_triple_element("expression", "efrbroo:F22_Self-Contained_Expression", "var")
        },
        {
            "subj": create_triple_element("expCreation", "efrbroo:F28_Expression_Creation", "var"),
            "pred": create_triple_element("ecrm:P9_consists_of", "ecrm:P9_consists_of", "uri"),
            "obj": create_triple_element("compositionActivity", "ecrm:E7_Activity", "var")
        },
        {
            "subj": create_triple_element("compositionActivity", "ecrm:E7_Activity", "var"),
            "pred": create_triple_element("ecrm:P14_carried_out_by", "ecrm:P14_carried_out_by", "uri"),
            "obj": create_triple_element("composer", "ecrm:E21_Person", "var")
        },
        {
            "subj": create_triple_element("compositionActivity", "ecrm:E7_Activity", "var"),
            "pred": create_triple_element("mus:U31_had_function", "mus:U31_had_function", "uri"),
            "obj": create_triple_element("composerFunction", "http://data.doremus.org/vocabulary/function/composer", "uri")
        },
        {
            "subj": create_triple_element("composer", "ecrm:E21_Person", "var"),
            "pred": create_triple_element("foaf:name", "foaf:name", "uri"),
            "obj": create_triple_element("composerName", "", "var")
        }
        ]
        filter_st = []
        logger.info(f"Composer name: {composer_name}, resolved: {resolved_composer}")
        
        if resolved_composer:
            # Add VALUES clause with comment
            triples.append({
            "subj": create_triple_element("composer", "ecrm:E21_Person", "var"),
            "pred": create_triple_element("VALUES", "VALUES", "uri"),
            "obj": create_triple_element(resolved_composer, resolved_composer, "uri")
        })
        elif composer_name:
            # Fallback to regex if resolution failed? 
            # User explicitly said "take the first URI... then add the URI to the query".
            # If resolution fails, maybe we should still try regex or just fail?
            # I'll add regex fallback for robustness.
            filter_st.append({'function': 'REGEX', 'args': ['?composerName', f"\'{composer_name}\'", "\'i\'"]})

        if composer_nationality:
            code = COUNTRY_CODES.get(composer_nationality.lower())
            if code:
                triples.append({
                    "subj": create_triple_element("composer", "ecrm:E21_Person", "var"),
                    "pred": create_triple_element("schema:birthPlace / geonames:countryCode", "schema:birthPlace / geonames:countryCode", "uri"),
                    "obj": create_triple_element(code, composer_nationality, "literal")
                })
            else:
                # Fallback if unknown code?
                logger.warning(f"Unknown nationality code for: {composer_nationality}")
        
        def_vars = qc.extract_defined_variables(triples)
        composer_module = {
            "id": "work_composer_filter",
            "type": "pattern",
            "scope": "main",
            "triples": triples,
            "filter_st": filter_st,
            "defined_vars": def_vars,
        }
        # Does not arrive here
        #logger.info(f"Adding composer module with triples: {triples} and filters: {filter_st}")
        await qc.add_module(composer_module)

    # Genre Filter
    if genre:
        triples = [
                {
                "subj": create_triple_element("expression", "efrbroo:F22_Self-Contained_Expression", "var"),
                "pred": create_triple_element("mus:U12_has_genre", "mus:U12_has_genre", "uri"),
                "obj": create_triple_element("genre", "", "var")
            }
        ]
        filter_st = []
        
        if resolved_genre:
            triples.append({
                "subj": create_triple_element("genre", "", "var"),
                "pred": create_triple_element("VALUES", "VALUES", "uri"),
                "obj": create_triple_element(resolved_genre, resolved_genre, "uri")
            })
        else:
            # Fallback
            triples.append({
                "subj": create_triple_element("genre", "", "var"),
                "pred": create_triple_element("skos:prefLabel", "skos:prefLabel", "uri"),
                "obj": create_triple_element("genreLabel", "", "var")
            })
            triples.append({'function': 'REGEX', 'args': ['?genreLabel', f"\'{genre}\'", "\'i\'"]})
             
        genre_module = {
            "id": "work_genre_filter",
            "type": "pattern",
            "scope": "main",
            "triples": triples,
            "filter_st": filter_st,
            "required_vars": [{"var_name": "expression", "var_label": "efrbroo:F22_Self-Contained_Expression"}],
            "defined_vars": [{"var_name": "genre", "var_label": resolved_genre if resolved_genre else ""}]
        }
        await qc.add_module(genre_module)
        
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
        triples.append({
                "subj": create_triple_element("expCreation", "efrbroo:F28_Expression_Creation", "var"),
                "pred": create_triple_element("efrbroo:R17_created", "efrbroo:R17_created", "uri"),
                "obj": create_triple_element("expression", "efrbroo:F22_Self-Contained_Expression", "var")
            }
        )
        triples.append({
                "subj": create_triple_element("expCreation", "efrbroo:F28_Expression_Creation", "var"),
                "pred": create_triple_element("ecrm:P7_took_place_at", "ecrm:P7_took_place_at", "uri"),
                "obj": create_triple_element("placeComp", "", "var")
            }
        )
        filter_st = []

        if resolved_place:
            triples.append({
                "subj": create_triple_element("placeComp", "", "var"),
                "pred": create_triple_element("VALUES", "VALUES", "uri"),
                "obj": create_triple_element(resolved_place, resolved_place, "uri")
            })
        else:
            triples.append({
                "subj": create_triple_element("placeComp", "", "var"),
                "pred": create_triple_element("rdfs:label", "rdfs:label", "uri"),
                "obj": create_triple_element("placeCompLabel", "", "var")
            })
            filter_st.append({'function': 'REGEX', 'args': ['?placeCompLabel', f"\'{place_of_composition}\'", "\'i\'"]})

        place_module = {
            "id": "work_place_filter",
            "type": "pattern",
            "scope": "main",
            "triples": triples,
            "filter_st": filter_st,
            "required_vars": [{"var_name": "expression", "var_label": "efrbroo:F22_Self-Contained_Expression"}],
            "defined_vars": [
                {"var_name": "expCreation", "var_label": "efrbroo:F28_Expression_Creation"},
                {"var_name": "placeComp", "var_label": resolved_place if resolved_place else ""}
            ]
        }
        await qc.add_module(place_module)

    # Key Filter
    if musical_key:
        triples = [{
                "subj": create_triple_element("expression", "efrbroo:F22_Self-Contained_Expression", "var"),
                "pred": create_triple_element("mus:U11_has_key", "mus:U11_has_key", "uri"),
                "obj": create_triple_element("key", "", "var")
            }
        ]
        filter_st = []
        if resolved_key:
            triples.append({
                "subj": create_triple_element("key", "", "var"),
                "pred": create_triple_element("VALUES", "VALUES", "uri"),
                "obj": create_triple_element(resolved_key, resolved_key, "uri")
            })
        else:
            triples.append({
                "subj": create_triple_element("key", "", "var"),
                "pred": create_triple_element("skos:prefLabel", "skos:prefLabel", "uri"),
                "obj": create_triple_element("keyLabel", "", "var")
            })
            filter_st.append({'function': 'REGEX', 'args': ['?keyLabel', f"\'{musical_key}\'", "\'i\'"]})
             
        key_module = {
            "id": "work_key_filter",
            "type": "pattern",
            "scope": "main",
            "triples": triples,
            "filter_st": filter_st,
            "required_vars": [{"var_name": "expression", "var_label": "efrbroo:F22_Self-Contained_Expression"}],
            "defined_vars": [{"var_name": "key", "var_label": resolved_key if resolved_key else ""}]
        }
        await qc.add_module(key_module)
        
    return qc

async def query_performance(
    query_id: str,
    question: str = "",
    title: Optional[str] = None,
    location: Optional[str] = None,
    carried_out_by: Optional[List[str]] = None,
    limit: int = 50
) -> QueryContainer:
    """
    Initialize a QueryContainer with a baseline query for Performances.
    """
    qc = QueryContainer(query_id, question)
    qc.set_limit(limit)
    
    # Select variables
    select_vars = [
        create_select_element("performance", "efrbroo:F31_Performance", False),
        create_select_element("title", "", True),
        create_select_element("locationName", "", True)
    ]
    
    qc.set_select(select_vars)
    
    # Core Module: Performance Entity
    core_module = {
        "id": "performance_core",
        "type": "where",
        "scope": "main",
        "triples": [
            {
                "subj": create_triple_element("performance", "efrbroo:F31_Performance", "var"),
                "pred": create_triple_element("a", "a", "uri"),
                "obj": create_triple_element("performance_type", "efrbroo:F31_Performance", "uri")
            },
            {
                "subj": create_triple_element("performance", "efrbroo:F31_Performance", "var"),
                "pred": create_triple_element("rdfsLabel", "rdfs:label", "uri"),
                "obj": create_triple_element("title", "", "var")
            }
        ]
    }
    await qc.add_module(core_module)
    
    # Title Filter (Advanced)
    if title:
        title_filter_module = {
            "id": "performance_title_filter",
            "type": "filter",
            "scope": "main",
            "filter_st": [
                {'function': 'REGEX', 'args': ['?title', f"\'{title}\'", "\'i\'"]}
            ],
        }
        await qc.add_module(title_filter_module)

    location_triples = []
    location_filters = []

    location_triples.append({
        "subj": create_triple_element("performance", "efrbroo:F31_Performance", "var"),
        "pred": create_triple_element("ecrm:P7_took_place_at", "ecrm:P7_took_place_at", "uri"),
        "obj": create_triple_element("place", "ecrm:E53_Place", "var")
    })
    
    # Location Filter
    if location:
        
        # Filter by String
        location_triples.append({
            "subj": create_triple_element("place", "ecrm:E53_Place", "var"),
            "pred": create_triple_element("rdfsLabel", "rdfs:label", "uri"),
            "obj": create_triple_element("locationName", "", "var")
        })
        location_filters.append({'function': 'REGEX', 'args': ['?locationName', f"\'{location}\'", "\'i\'"]})

        loc_module = {
            "id": "performance_location_filter",
            "type": "pattern",
            "scope": "main",
            "triples": location_triples,
            "filter_st": location_filters,
            "required_vars": [{"var_name": "performance", "var_label": "efrbroo:F31_Performance"}],
            "defined_vars": [
                {"var_name": "place", "var_label": "ecrm:E53_Place"},
                {"var_name": "locationName", "var_label": ""}
            ]
        }
        await qc.add_module(loc_module)
    else:
        # Optional location for SELECT
        location_triples.append({
            "subj": create_triple_element("place", "ecrm:E53_Place", "var"),
            "pred": create_triple_element("rdfsLabel", "rdfs:label", "uri"),
            "obj": create_triple_element("locationName", "", "var")
        })
        loc_opt_module = {
            "id": "performance_location_optional",
            "type": "optional",
            "scope": "optional",
            "triples": location_triples,
            "required_vars": [{"var_name": "performance", "var_label": "efrbroo:F31_Performance"}],
            "defined_vars": [
                {"var_name": "place", "var_label": "ecrm:E53_Place"},
                {"var_name": "locationName", "var_label": ""}
            ]
        }
        await qc.add_module(loc_opt_module)

    # Performer Filter (carried_out_by)
    if carried_out_by:
        for idx, person_name in enumerate(carried_out_by):
            # Resolve if possible
            resolved_uri = _resolve_entity(person_name, "artist")
            
            # The pattern is recursive/deep: ?performance -> consists_of* -> activity -> carried_out_by -> artist
            # We use a path that covers both conductors and musicians
            
            # Dynamic variable names based on index to avoid collisions
            activity_var = f"activity_{idx}"
            artist_var = f"artist_{idx}"
            artist_name_var = f"artistName_{idx}"

            performer_triples = []
            performer_filters = []
            
            # Use property path for flexibility
            # 1. Performance -> Activity (Recursive Path)
            performer_triples.append({
                "subj": create_triple_element("performance", "efrbroo:F31_Performance", "var"),
                "pred": create_triple_element("ecrm:P9_consists_of_plus", "ecrm:P9_consists_of+", "uri"),
                "obj": create_triple_element(activity_var, "ecrm:E7_Activity", "var")
            })
            
            # 2. Activity -> Artist
            performer_triples.append({
                "subj": create_triple_element(activity_var, "ecrm:E7_Activity", "var"),
                "pred": create_triple_element("ecrm:P14_carried_out_by", "ecrm:P14_carried_out_by", "uri"),
                "obj": create_triple_element(artist_var, "ecrm:E21_Person", "var")
            })
            
            if resolved_uri:
                performer_triples.append({
                    "subj": create_triple_element(artist_var, "ecrm:E21_Person", "var"),
                    "pred": create_triple_element("VALUES", "VALUES", "uri"),
                    "obj": create_triple_element(resolved_uri, resolved_uri, "uri")
                })
            else:
                performer_triples.append({
                    "subj": create_triple_element(artist_var, "ecrm:E21_Person", "var"),
                    "pred": create_triple_element("foaf:name", "foaf:name", "uri"),
                    "obj": create_triple_element(artist_name_var, "", "var")
                })
                performer_filters.append({'function': 'REGEX', 'args': [f'?{artist_name_var}', f"\'{person_name}\'", "\'i\'"]})

            perf_module = {
                "id": f"performance_artist_{idx}",
                "type": "pattern",
                "scope": "main",
                "triples": performer_triples,
                "filter_st": performer_filters,
                "required_vars": [{"var_name": "performance", "var_label": "efrbroo:F31_Performance"}],
            }
            await qc.add_module(perf_module)
        
    return qc

async def query_artist(
    query_id: str,
    question: str = "",
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
    qc = QueryContainer(query_id, question)
    qc.set_limit(limit)
    
    # Select variables
    select_vars = [
        {"var_name": "artist", "var_label": "ecrm:E21_Person", "is_sample": False},
        {"var_name": "name", "var_label": "", "is_sample": True}
    ]
    qc.set_select(select_vars)
    
    # Core Module: Artist Entity
    core_module = {
        "id": "artist_core",
        "type": "where",
        "scope": "main",
        "triples": [
            {
                "subj": create_triple_element("artist", "ecrm:E21_Person", "var"),
                "pred": create_triple_element("a", "a", "uri"),
                "obj": create_triple_element("artist_type", "ecrm:E21_Person", "uri")
            },
            {
                "subj": create_triple_element("artist", "ecrm:E21_Person", "var"),
                "pred": create_triple_element("rdfsLabel", "rdfs:label", "uri"),
                "obj": create_triple_element("name", "", "var")
            }
        ]
    }
    await qc.add_module(core_module)
    
    # Name Filter
    if name:
        resolved_artist = _resolve_entity(name, "composer")
        triples = []
        filter_st = []

        if resolved_artist:
            triples.append({
                "subj": create_triple_element("artist", "ecrm:E21_Person", "var"),
                "pred": create_triple_element("VALUES", "VALUES", "uri"),
                "obj": create_triple_element(resolved_artist, resolved_artist, "uri")
            })
        else:
            filter_st.append({'function': 'REGEX', 'args': ['?name', f"\'{name}\'", "\'i\'"]})
        
        name_module = {
            "id": "artist_name_filter",
            "type": "pattern",
            "scope": "main",
            "triples": triples,
            "filter_st": filter_st,
            "required_vars": [{"var_name": "artist", "var_label": "ecrm:E21_Person"}]
        }
        await qc.add_module(name_module)

    # Nationality Filter
    if nationality:
         country_code = COUNTRY_CODES.get(nationality.lower())
         if country_code:
             nat_module = {
                "id": "artist_nationality_filter",
                "type": "filter",
                "scope": "main",
                "triples": [
                    {
                        "subj": create_triple_element("artist", "ecrm:E21_Person", "var"),
                        "pred": create_triple_element("birthPlaceCountry", "schema:birthPlace / geonames:countryCode", "uri"),
                        "obj": create_triple_element(country_code, nationality, "literal")
                    }
                ],
                "required_vars": [{"var_name": "artist", "var_label": "ecrm:E21_Person"}]
             }
             await qc.add_module(nat_module)
         else:
             logger.warning(f"Unknown nationality code for: {nationality}")

    # Birth Place: ?artist schema:birthPlace ?bp . ?bp rdfs:label ?bpLabel
    if birth_place:
        triples = [
            {
            "subj": create_triple_element("artist", "ecrm:E21_Person", "var"),
            "pred": create_triple_element("schema:birthPlace", "schema:birthPlace", "uri"),
            "obj": create_triple_element("bp", "", "var")
            },
            {
            "subj": create_triple_element("bp", "", "var"),
            "pred": create_triple_element("rdfsLabel", "rdfs:label", "uri"),
            "obj": create_triple_element("bpLabel", "", "var")
            }
        ]
        filter_st = [{'function': 'REGEX', 'args': ['?bpLabel', f"\'{birth_place}\'", "\'i\'"]}]
        
        bp_module = {
            "id": "artist_birth_place_filter",
            "type": "pattern",
            "scope": "main",
            "triples": triples,
            "filter_st": filter_st,
            "required_vars": [{"var_name": "artist", "var_label": "ecrm:E21_Person"}],
            "defined_vars": [
                {"var_name": "bp", "var_label": ""},
                {"var_name": "bpLabel", "var_label": ""}
            ]
        }
        await qc.add_module(bp_module)

    # Death Place: ?artist schema:deathPlace ?dp . ?dp rdfs:label ?dpLabel
    if death_place:
        triples = [
            {
            "subj": create_triple_element("artist", "ecrm:E21_Person", "var"),
            "pred": create_triple_element("schema:deathPlace", "schema:deathPlace", "uri"),
            "obj": create_triple_element("dp", "", "var")
            },
            {
            "subj": create_triple_element("dp", "", "var"),
            "pred": create_triple_element("rdfsLabel", "rdfs:label", "uri"),
            "obj": create_triple_element("dpLabel", "", "var")
            }
        ]
        filter_st = [{'function': 'REGEX', 'args': ['?dpLabel', f"\'{death_place}\'", "\'i\'"]}]
        
        dp_module = {
            "id": "artist_death_place_filter",
            "type": "filter",
            "scope": "main",
            "triples": triples,
            "filter_st": filter_st,
            "required_vars": [{"var_name": "artist", "var_label": "ecrm:E21_Person"}],
            "defined_vars": [
                {"var_name": "dp", "var_label": ""},
                {"var_name": "dpLabel", "var_label": ""}
            ]
        }
        await qc.add_module(dp_module)

    # Work Name Filter
    if work_name:
        triples = [
            {
            "subj": create_triple_element("performanceWork", "efrbroo:F28_Expression_Creation", "var"),
            "pred": create_triple_element("ecrm:P9_consists_of / ecrm:P14_carried_out_by", "ecrm:P9_consists_of / ecrm:P14_carried_out_by", "uri"),
            "obj": create_triple_element("artist", "ecrm:E21_Person", "var")
            },
            {
            "subj": create_triple_element("performanceWork", "efrbroo:F28_Expression_Creation", "var"),
            "pred": create_triple_element("efrbroo:R17_created", "efrbroo:R17_created", "uri"),
            "obj": create_triple_element("expression", "efrbroo:F22_Self-Contained_Expression", "var")
            },
            {
            "subj": create_triple_element("expression", "efrbroo:F22_Self-Contained_Expression", "var"),
            "pred": create_triple_element("rdfsLabel", "rdfs:label", "uri"),
            "obj": create_triple_element("workTitle", "", "var")
            }
        ]
        filter_st = [{'function': 'REGEX', 'args': ['?workTitle', f"\'{work_name}\'", "\'i\'"]}]
        
        work_module = {
            "id": "artist_work_filter",
            "type": "pattern",
            "scope": "main",
            "triples": triples,
            "filter_st": filter_st,
            "required_vars": [{"var_name": "artist", "var_label": "ecrm:E21_Person"}],
            "defined_vars": [
                {"var_name": "performanceWork", "var_label": "efrbroo:F28_Expression_Creation"},
                {"var_name": "expression", "var_label": "efrbroo:F22_Self-Contained_Expression"},
                {"var_name": "workTitle", "var_label": ""}
            ]
        }
        await qc.add_module(work_module)

    return qc
