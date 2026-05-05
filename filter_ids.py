import csv

STOP_IDS = {
    "1127036",
    "1126954",
    "1127037",
    "1127034",
    "1127033"

}

INPUT_FILE = "stop_times.txt"
OUTPUT_FILE = "stop_times_filtered.txt"

with open(INPUT_FILE, "r", encoding="utf-8-sig", newline="") as infile, \
     open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as outfile:

    reader = csv.DictReader(infile)
    writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)

    writer.writeheader()

    for row in reader:
        if row["stop_id"] in STOP_IDS:
            writer.writerow(row)

print(f"Filtered rows written to {OUTPUT_FILE}")