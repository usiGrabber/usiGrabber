#!/usr/bin/env python3
"""
Fetch all projects from PRIDE Archive API and save to JSON file.
"""

import json

import requests


def fetch_pride_projects():
    """Fetch all projects from PRIDE Archive API."""
    base_url = "https://www.ebi.ac.uk/pride/ws/archive/v3"
    endpoint = "projects/all"
    url = f"{base_url}/{endpoint}"

    headers = {"accept": "application/json"}

    print(f"Fetching data from {url}...")
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    data = response.json()

    # Save to JSON file
    output_file = "experimental/pride_projects_all.json"
    with open(output_file, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Successfully saved data to {output_file}")
    print(f"Total projects: {len(data) if isinstance(data, list) else 'N/A'}")


if __name__ == "__main__":
    fetch_pride_projects()
