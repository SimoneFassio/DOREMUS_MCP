from typing import Optional, List, Dict, Any, Union
import logging
import re
from server.query_container import QueryContainer, create_triple_element
from server.utils import resolve_entity_uri
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

async def query_works(
    query_id: str,
    question: str = "",
    title: Optional[str] = None,
    composer_name: Optional[str] = None,
    composer_nationality: Optional[str] = None,
    genre: Optional[str] = None,
    place_of_composition: Optional[str] = None,
    musical_key: Optional[str] = None,
) -> QueryContainer:
    """
    Initialize a QueryContainer with a baseline query for Musical Works.
    """
    qc = QueryContainer(query_id, question)
    
    # Define logging callback
    def log_sampling(log_data: Dict[str, Any]):
        qc.sampling_logs.append(log_data)
    
    # 2. Variable Resolvers
    resolved_composer = await resolve_entity_uri(composer_name, "artist", question, log_sampling) if composer_name else None
    resolved_genre = await resolve_entity_uri(genre, "vocabulary", question, log_sampling) if genre else None
    resolved_place = await resolve_entity_uri(place_of_composition, "place", question, log_sampling) if place_of_composition else None
    resolved_key = await resolve_entity_uri(musical_key, "vocabulary", question, log_sampling) if musical_key else None

    # 3. Core Module: Expression & Title
    core_module = {
        "id": "work_core",
        "type": "query_builder",
        "scope": "main",
        "triples": [
            {"subj": create_triple_element("expression", "efrbroo:F22_Self-Contained_Expression", "var"), 
             "pred": create_triple_element("a", "a", "uri"),
             "obj": create_triple_element("expression_type", "efrbroo:F22_Self-Contained_Expression", "uri")},
            {"subj": create_triple_element("expression", "efrbroo:F22_Self-Contained_Expression", "var"),
             "pred": create_triple_element("rdfsLabel", "rdfs:label", "uri"),
             "obj": create_triple_element("title", "", "var")},
             {"subj": create_triple_element("expCreation", "efrbroo:F28_Expression_Creation", "var"),
             "pred": create_triple_element("efrbroo:R17_created", "efrbroo:R17_created", "uri"),
             "obj": create_triple_element("expression", "efrbroo:F22_Self-Contained_Expression", "var")}
        ],
    }
    await qc.add_module(core_module)

    qc.add_select("expression", "efrbroo:F22_Self-Contained_Expression")
    
    # 4. Filter Modules
    
    # Title Filter
    if title:
        title_filter_module = {
            "id": "work_title_filter",
            "type": "query_builder",
            "scope": "main",
            "triples": [],
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
            triples.append({
            "subj": create_triple_element("composer", "ecrm:E21_Person", "var"),
            "pred": create_triple_element("VALUES", "VALUES", "uri"),
            "obj": create_triple_element(resolved_composer, resolved_composer, "uri")
        })
        elif composer_name:
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
                raise ValueError(f"Unknown nationality: {composer_nationality}, available: {COUNTRY_CODES.keys()}")
        
        composer_module = {
            "id": "work_composer_filter",
            "type": "query_builder",
            "scope": "main",
            "triples": triples,
            "filter_st": filter_st
        }
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
            filter_st.append({'function': 'REGEX', 'args': ['?genreLabel', f"\'{genre}\'", "\'i\'"]})
             
        genre_module = {
            "id": "work_genre_filter",
            "type": "query_builder",
            "scope": "main",
            "triples": triples,
            "filter_st": filter_st
        }
        await qc.add_module(genre_module)
        
    # Place of Composition
    if place_of_composition:
        # Pattern:
        # ?expCreation ecrm:P7_took_place_at ?placeComp .
        triples = []
        triples.append({
                "subj": create_triple_element("expCreation", "efrbroo:F28_Expression_Creation", "var"),
                "pred": create_triple_element("ecrm:P7_took_place_at", "ecrm:P7_took_place_at", "uri"),
                "obj": create_triple_element("placeComp", "", "var")
            })
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
            "type": "query_builder",
            "scope": "main",
            "triples": triples,
            "filter_st": filter_st
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
            "type": "query_builder",
            "scope": "main",
            "triples": triples,
            "filter_st": filter_st
        }
        await qc.add_module(key_module)
        
    return qc

async def query_performance(
    query_id: str,
    question: str = "",
    title: Optional[str] = None,
    location: Optional[str] = None,
    carried_out_by: Optional[List[str]] = None
) -> QueryContainer:
    """
    Initialize a QueryContainer with a baseline query for Performances.
    """
    qc = QueryContainer(query_id, question)

    # Define logging callback
    def log_sampling(log_data: Dict[str, Any]):
        qc.sampling_logs.append(log_data)
    
    # Core Module: Performance Entity
    core_module = {
        "id": "performance_core",
        "type": "query_builder",
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
        ],
    }
    await qc.add_module(core_module)
    qc.add_select("performance", "efrbroo:F31_Performance")
    
    # Title Filter (Advanced)
    if title:
        title_filter_module = {
            "id": "performance_title_filter",
            "type": "query_builder",
            "scope": "main",
            "triples": [],
            "filter_st": [
                {'function': 'REGEX', 'args': ['?title', f"\'{title}\'", "\'i\'"]}
            ]
        }
        await qc.add_module(title_filter_module)
    
    # Location Filter
    if location:
        location_triples = []
        location_filters = []

        location_triples.append({
            "subj": create_triple_element("performance", "efrbroo:F31_Performance", "var"),
            "pred": create_triple_element("ecrm:P7_took_place_at", "ecrm:P7_took_place_at", "uri"),
            "obj": create_triple_element("place", "ecrm:E53_Place", "var")
        })
        resolved_uri = await resolve_entity_uri(location, "place", question, log_sampling)

        if resolved_uri:
            location_triples.append({
                "subj": create_triple_element("place", "ecrm:E53_Place", "var"),
                "pred": create_triple_element("VALUES", "VALUES", "uri"),
                "obj": create_triple_element(resolved_uri, resolved_uri, "uri")
            })
        
        else:
            # Filter by String
            location_triples.append({
                "subj": create_triple_element("place", "ecrm:E53_Place", "var"),
                "pred": create_triple_element("rdfsLabel", "rdfs:label", "uri"),
                "obj": create_triple_element("locationName", "", "var")
            })
            location_filters.append({'function': 'REGEX', 'args': ['?locationName', f"\'{location}\'", "\'i\'"]})

        loc_module = {
            "id": "performance_location_filter",
            "type": "query_builder",
            "scope": "main",
            "triples": location_triples,
            "filter_st": location_filters
        }
        await qc.add_module(loc_module)

    # Performer Filter (carried_out_by)
    if carried_out_by:
        for idx, person_name in enumerate(carried_out_by):
            # Resolve if possible
            resolved_uri = await resolve_entity_uri(person_name, "artist", question, log_sampling)
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
                "pred": create_triple_element("ecrm:P9_consists_of+", "ecrm:P9_consists_of+", "uri"),
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
                "type": "query_builder",
                "scope": "main",
                "triples": performer_triples,
                "filter_st": performer_filters
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
) -> QueryContainer:
    """
    Initialize a QueryContainer with a baseline query for Artists.
    """
    qc = QueryContainer(query_id, question)
    
    # Define logging callback
    def log_sampling(log_data: Dict[str, Any]):
        qc.sampling_logs.append(log_data)
    
    # Core Module: Artist Entity
    core_module = {
        "id": "artist_core",
        "type": "query_builder",
        "scope": "main",
        "triples": [
            {
                "subj": create_triple_element("artist", "ecrm:E21_Person", "var"),
                "pred": create_triple_element("a", "a", "uri"),
                "obj": create_triple_element("artist_type", "ecrm:E21_Person", "uri")
            },
            {
                "subj": create_triple_element("artist", "ecrm:E21_Person", "var"),
                "pred": create_triple_element("name", "foaf:name", "uri"),
                "obj": create_triple_element("name", "", "var")
            }
        ]
    }
    await qc.add_module(core_module)
    qc.add_select("artist", "ecrm:E21_Person")
    # Name Filter
    if name:
        resolved_artist = await resolve_entity_uri(name, "artist", question, log_sampling)
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
            "type": "query_builder",
            "scope": "main",
            "triples": triples,
            "filter_st": filter_st
        }
        await qc.add_module(name_module)

    # Nationality Filter
    if nationality:
         country_code = COUNTRY_CODES.get(nationality.lower())
         if country_code:
             nat_module = {
                "id": "artist_nationality_filter",
                "type": "query_builder",
                "scope": "main",
                "triples": [
                    {
                        "subj": create_triple_element("artist", "ecrm:E21_Person", "var"),
                        "pred": create_triple_element("birthPlaceCountry", "schema:birthPlace / geonames:countryCode", "uri"),
                        "obj": create_triple_element(country_code, nationality, "literal")
                    }
                ]
             }
             await qc.add_module(nat_module)
         else:
             raise ValueError(f"Unknown nationality: {nationality}, list of available nationalities: {list(COUNTRY_CODES.keys())}")

    # Birth Place: ?artist schema:birthPlace ?bp . ?bp rdfs:label ?bpLabel
    if birth_place:
        resolved_bp = await resolve_entity_uri(birth_place, "place", question, log_sampling)
        triples = [
            {
            "subj": create_triple_element("artist", "ecrm:E21_Person", "var"),
            "pred": create_triple_element("schema:birthPlace", "schema:birthPlace", "uri"),
            "obj": create_triple_element("bp", "", "var")
            }
        ]
        filter_st = []

        if resolved_bp:
            triples.append({
                "subj": create_triple_element("bp", "", "var"),
                "pred": create_triple_element("VALUES", "VALUES", "uri"),
                "obj": create_triple_element(resolved_bp, resolved_bp, "uri")
            })
        else:
            triples.append({
                "subj": create_triple_element("bp", "", "var"),
                "pred": create_triple_element("rdfsLabel", "rdfs:label", "uri"),
                "obj": create_triple_element("bpLabel", "", "var")
            })
            filter_st = [{'function': 'REGEX', 'args': ['?bpLabel', f"\'{birth_place}\'", "\'i\'"]}]
        
        bp_module = {
            "id": "artist_birth_place_filter",
            "type": "query_builder",
            "scope": "main",
            "triples": triples,
            "filter_st": filter_st
        }
        await qc.add_module(bp_module)

    # Death Place: ?artist schema:deathPlace ?dp . ?dp rdfs:label ?dpLabel
    if death_place:
        resolved_dp = await resolve_entity_uri(death_place, "place", question, log_sampling)
        triples = [{
            "subj": create_triple_element("artist", "ecrm:E21_Person", "var"),
            "pred": create_triple_element("schema:deathPlace", "schema:deathPlace", "uri"),
            "obj": create_triple_element("dp", "", "var")
            }]
        filter_st = []

        if resolved_dp:
            triples.append({
                "subj": create_triple_element("dp", "", "var"),
                "pred": create_triple_element("VALUES", "VALUES", "uri"),
                "obj": create_triple_element(resolved_dp, resolved_dp, "uri")
            })
        else:
            triples.append({
                "subj": create_triple_element("dp", "", "var"),
                "pred": create_triple_element("rdfsLabel", "rdfs:label", "uri"),
                "obj": create_triple_element("dpLabel", "", "var")
            })
            filter_st = [{'function': 'REGEX', 'args': ['?dpLabel', f"\'{death_place}\'", "\'i\'"]}]
        
        dp_module = {
            "id": "artist_death_place_filter",
            "type": "query_builder",
            "scope": "main",
            "triples": triples,
            "filter_st": filter_st
        }
        await qc.add_module(dp_module)

    # Work Name Filter
    if work_name:
        resolved_work = await resolve_entity_uri(work_name, "others", question, log_sampling)
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

        filter_st = []
        if resolved_work:
            triples.append({
                "subj": create_triple_element("expression", "efrbroo:F22_Self-Contained_Expression", "var"),
                "pred": create_triple_element("VALUES", "VALUES", "uri"),
                "obj": create_triple_element(resolved_work, resolved_work, "uri")
            })
        else:
            filter_st = [{'function': 'REGEX', 'args': ['?workTitle', f"\'{work_name}\'", "\'i\'"]}]
        
        work_module = {
            "id": "artist_work_filter",
            "type": "query_builder",
            "scope": "main",
            "triples": triples,
            "filter_st": filter_st
        }
        await qc.add_module(work_module)

    return qc
