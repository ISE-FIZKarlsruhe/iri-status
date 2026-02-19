import requests
import csv
from rdflib import Graph
from rdflib.util import guess_format
from rdflib.namespace import OWL
import pandas as pd

RDF_MEDIA_TYPES = {
    "application/rdf+xml": "xml",
    "text/turtle": "turtle",
    "application/ld+json": "json-ld",
    "application/n-triples": "nt",
    "application/n-quads": "nquads",
    "text/n3": "n3"
}

ACCEPT_HEADERS = [
    "application/rdf+xml",
    "text/turtle",
    "application/ld+json",
    "application/n-triples",
    "text/n3"   
]


def try_parse_rdf(content, content_type):
    media_type = content_type.split(";")[0].strip()
    rdf_format = RDF_MEDIA_TYPES.get(media_type) or guess_format(media_type)

    if not rdf_format:
        return False, 0, "Unknown format"

    g = Graph()
    try:
        g.parse(data=content, format=rdf_format)
        return True, len(g), "Parsed", g
    except Exception as e:
        return False, 0, f"Parse error: {e}", g

def get_version_info(graph):
    """
    Extract versionIRI and priorVersion from ontology.
    """
    version_iri = None
    prior_version = None

    for s in graph.subjects(predicate=None, object=OWL.Ontology):
        # Find versionIRI
        for v in graph.objects(s, OWL.versionIRI):
            version_iri = str(v)
        # Find priorVersion
        for p in graph.objects(s, OWL.priorVersion):
            prior_version = str(p)

    return version_iri, prior_version

def test_iri(iri):
    results = []

    for accept in ACCEPT_HEADERS:
        headers = {"Accept": accept}

        try:
            response = requests.get(
                iri,
                headers=headers,
                timeout=15,
                allow_redirects=True
            )

            status = response.status_code
            final_url = response.url
            content_type = response.headers.get("Content-Type", "Unknown")

            if status == 200:
                parsed, triple_count, message, graph = try_parse_rdf(
                    response.content,
                    content_type
                )
                curr_version, prior_version = get_version_info(graph)
            else:
                parsed = False
                triple_count = 0
                message = f"HTTP {status}"
                curr_version = ""
                prior_version = ""

        except Exception as e:
            status = "ERROR"
            final_url = ""
            content_type = ""
            parsed = False
            triple_count = 0
            curr_version = ""
            prior_version = ""
            message = str(e)

        results.append({
            "iri": iri,
            "accept_header": accept,
            "http_status": status,
            "final_url": final_url,
            "content_type": content_type,
            "parsed_successfully": parsed,
            "current_version": curr_version,
            "previous_version": prior_version,
            "triple_count": triple_count,
            "message": message
        })

    return results


def process_file(input_file, output_file):
    all_results = []

    with open(input_file, "r") as f:
        iris = [line.strip() for line in f if line.strip()]

    for iri in iris:
        print(f"Processing: {iri}")
        results = test_iri(iri)
        all_results.extend(results)

    fieldnames = [
        "iri",
        "accept_header",
        "http_status",
        "final_url",
        "content_type",
        "parsed_successfully",
        "current_version",
        "previous_version",
        "triple_count",
        "message"
    ]

    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)

    print(f"\nResults written to {output_file}")

def csv_to_md(input_csv, output_md):
    # Read CSV
    df = pd.read_csv(input_csv)

    # Convert to Markdown table
    markdown_table = df.to_markdown(index=False)

    # Write to file
    with open(output_md, "w", encoding="utf-8") as f:
        f.write(markdown_table)

    print(f"Markdown file written to {output_md}")


if __name__ == "__main__":
    input_file = "iri.csv"
    output_file = "ontology_results.csv"
    process_file(input_file, output_file)
    csv_to_md("ontology_results.csv", "ontology_results.md")
