import logging
import xml.etree.ElementTree as ET
from pathlib import Path

logger = logging.Logger(__name__)


def shrink_owl_file(input_path: Path, output_path: Path):
    """
    Parses an OWL file and keeps only the header, ontology info,
    and owl:Class definitions with their rdfs:subClassOf and rdfs:label.
    """

    logger.info(f"Starting to shrink '{input_path}'...")

    try:
        original_tree = ET.parse(input_path)
        original_root = original_tree.getroot()

        for key, value in original_root.attrib.items():
            if key.startswith("xmlns"):
                prefix = ""
                if ":" in key:
                    prefix = key.split(":")[-1]
                ET.register_namespace(prefix, value)

        new_root = ET.Element(original_root.tag, original_root.attrib)

        for element in original_root:
            tag_name = element.tag.split("}")[-1]

            if tag_name == "Ontology":
                new_root.append(element)

            elif tag_name == "Class":
                new_class = ET.Element(element.tag, element.attrib)

                for prop in element:
                    prop_tag = prop.tag.split("}")[-1]

                    if prop_tag == "subClassOf" or prop_tag == "label":
                        new_class.append(prop)

                new_root.append(new_class)

        new_tree = ET.ElementTree(new_root)

        logger.info(f"Writing shrunk file to '{output_path}'...")
        new_tree.write(output_path, encoding="utf-8", xml_declaration=True)
        logger.info("\nDone! File has been shrunk successfully.")

    except ET.ParseError as e:
        logger.error("\nError: Failed to parse the XML file. Make sure it is valid XML.")
        logger.error(f"Details: {e}")
    except FileNotFoundError:
        logger.error(f"\nError: Input file not found at '{input_path}'")
    except Exception as e:
        logger.error(f"\nAn unexpected error occurred: {e}")
