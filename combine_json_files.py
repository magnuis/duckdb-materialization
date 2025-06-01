#!/usr/bin/env python3
"""
Script to combine all cleaned JSON files from twitter/json_without_null 
into one large JSONL file. Each line contains one JSON object.
"""

import os
import json
from pathlib import Path
import argparse


def combine_json_files(input_dir: str = "data/twitter/json_without_null",
                       output_file: str = "data/twitter/combined_data.jsonl",
                       include_metadata: bool = True) -> bool:
    """
    Combine all JSON files from subdirectories into one large JSONL file.

    Args:
        input_dir: Directory containing the cleaned JSON files
        output_file: Output JSONL file path
        include_metadata: Whether to include source file metadata

    Returns:
        True if successful, False otherwise
    """

    input_path = Path(input_dir)
    output_path = Path(output_file)

    if not input_path.exists():
        print(f"Error: Input directory {input_path} does not exist!")
        return False

    # Get all subdirectories
    subdirs = [d for d in input_path.iterdir() if d.is_dir()]

    if not subdirs:
        print(f"No subdirectories found in {input_path}")
        return False

    print(f"Combining JSON files from {input_path}")
    print(f"Output file: {output_path}")
    print("-" * 50)

    total_records = 0
    file_count = 0

    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(output_path, 'w', encoding='utf-8') as outfile:
            # Process each subdirectory
            for subdir in sorted(subdirs):
                print(f"Processing directory: {subdir.name}")

                # Get all JSON files in the subdirectory
                json_files = list(subdir.glob("*.json"))

                if not json_files:
                    print(f"  No JSON files found in {subdir}")
                    continue

                for json_file in sorted(json_files):
                    file_count += 1
                    records_in_file = 0

                    try:
                        with open(json_file, 'r', encoding='utf-8') as infile:
                            for line_num, line in enumerate(infile, 1):
                                line = line.strip()
                                if not line:
                                    continue

                                try:
                                    json_obj = json.loads(line)

                                    # Add metadata if requested
                                    if include_metadata:
                                        json_obj['_source_dir'] = subdir.name
                                        json_obj['_source_file'] = json_file.name
                                        json_obj['_line_number'] = line_num

                                    # Write to output file
                                    outfile.write(json.dumps(
                                        json_obj, separators=(',', ':')) + '\n')
                                    records_in_file += 1
                                    total_records += 1

                                except json.JSONDecodeError as e:
                                    print(
                                        f"  Warning: Invalid JSON on line {line_num} in {json_file}: {e}")
                                    continue

                        print(f"  {json_file.name}: {records_in_file} records")

                    except Exception as e:
                        print(f"  Error reading {json_file}: {e}")
                        continue

        print(f"\n=== Combination Complete ===")
        print(f"Files processed: {file_count}")
        print(f"Total records: {total_records}")
        print(f"Output file: {output_path}")
        print(f"File size: {output_path.stat().st_size / 1024 / 1024:.2f} MB")

        return True

    except Exception as e:
        print(f"Error writing output file: {e}")
        return False


def main():
    """Main function with command line argument support."""
    parser = argparse.ArgumentParser(
        description="Combine JSON files into one JSONL file")
    parser.add_argument("--input-dir", default="data/twitter/json_without_null",
                        help="Input directory containing JSON files")
    parser.add_argument("--output-file", default="data/twitter/combined_data.jsonl",
                        help="Output JSONL file path")
    parser.add_argument("--no-metadata", action="store_true",
                        help="Don't include source file metadata")

    args = parser.parse_args()

    print("JSON to JSONL Combiner")
    print("=" * 30)

    success = combine_json_files(
        input_dir=args.input_dir,
        output_file=args.output_file,
        include_metadata=not args.no_metadata
    )

    if success:
        print("\n✅ Files combined successfully!")
        print(f"Combined data saved to: {args.output_file}")
    else:
        print("\n❌ Combination failed!")


if __name__ == "__main__":
    main()
