import csv
import os
import sys
import xml.etree.ElementTree as ET

def osm_to_tsv(osm_path: str, out_tsv: str) -> None:
    if not os.path.isfile(osm_path):
        raise FileNotFoundError(f"OSM file not found: {osm_path}")

    os.makedirs(os.path.dirname(out_tsv), exist_ok=True)

    # TSV je odličan "među-format": jednostavan, stream-friendly
    with open(out_tsv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["id", "type", "lat", "lon", "k", "v"])

        count_rows = 0

        for _, elem in ET.iterparse(osm_path, events=("end",)):
            if elem.tag in ("node", "way"):
                obj_id = elem.attrib.get("id")
                obj_type = elem.tag
                lat = elem.attrib.get("lat")  # može biti None za way
                lon = elem.attrib.get("lon")

                for tag in elem.findall("tag"):
                    k = tag.attrib.get("k")
                    v = tag.attrib.get("v")
                    w.writerow([obj_id, obj_type, lat, lon, k, v])
                    count_rows += 1

                elem.clear()

        print(f"Done. Wrote {count_rows} rows to {out_tsv}")

if __name__ == "__main__":
    # usage: python src/osm_to_tsv.py data/processed/region_focus.osm data/processed/osm.tsv
    if len(sys.argv) != 3:
        print("Usage: python src/osm_to_tsv.py <input.osm> <output.tsv>")
        sys.exit(1)
    osm_to_tsv(sys.argv[1], sys.argv[2])
