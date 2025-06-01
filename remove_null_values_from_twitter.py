#!/usr/bin/env python3
"""
Script to remove null values from JSON files in twitter/json directory.
Processes all subdirectories (00, 01, 02, 03, 04) and their JSON files.
Saves cleaned data to twitter/json_without_null with the same structure.
"""

import os
import json
import shutil
from pathlib import Path
from typing import Dict, Any


def remove_null_values(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove fields with null values from a JSON object.

    Args:
        obj: Dictionary representing a JSON object

    Returns:
        Dictionary with null-valued fields removed
    """
    if not isinstance(obj, dict):
        return obj

    cleaned_obj = {}
    for key, value in obj.items():
        if value is not None:
            if isinstance(value, dict):
                cleaned_value = remove_null_values(value)
                if cleaned_value:  # Only add if the nested object is not empty
                    cleaned_obj[key] = cleaned_value
            elif isinstance(value, list):
                # Handle lists - remove null items and clean nested objects
                cleaned_list = []
                for item in value:
                    if item is not None:
                        if isinstance(item, dict):
                            cleaned_item = remove_null_values(item)
                            if cleaned_item:
                                cleaned_list.append(cleaned_item)
                        else:
                            cleaned_list.append(item)
                if cleaned_list:
                    cleaned_obj[key] = cleaned_list
            else:
                cleaned_obj[key] = value

    return cleaned_obj


def process_json_file(input_file: Path, output_file: Path):
    """
    Process a single JSON file to remove null values.

    Args:
        input_file: Path to input JSON file
        output_file: Path to output JSON file
    """
    try:
        with open(input_file, 'r', encoding='utf-8') as infile:
            lines = infile.readlines()

        cleaned_lines = []
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue

            try:
                # Parse JSON object
                json_obj = json.loads(line)

                # Remove null values
                cleaned_obj = remove_null_values(json_obj)

                # Only keep the line if there are remaining fields after cleaning
                if cleaned_obj:
                    cleaned_lines.append(json.dumps(
                        cleaned_obj, separators=(',', ':')))

            except json.JSONDecodeError as e:
                print(
                    f"Warning: Invalid JSON on line {line_num} in {input_file}: {e}")
                continue

        # Write cleaned data to output file
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as outfile:
            for line in cleaned_lines:
                outfile.write(line + '\n')

        print(
            f"Processed: {input_file} -> {output_file} ({len(cleaned_lines)} valid lines)")

    except Exception as e:
        print(f"Error processing {input_file}: {e}")


def process_all_files():
    """Process all JSON files in the twitter/json directory structure."""

    input_base = Path("data/twitter/json")
    output_base = Path("data/twitter/json_without_null")

    if not input_base.exists():
        print(f"Error: Input directory {input_base} does not exist!")
        return False

    # Get all subdirectories (00, 01, 02, 03, 04)
    subdirs = [d for d in input_base.iterdir() if d.is_dir()]

    if not subdirs:
        print(f"No subdirectories found in {input_base}")
        return False

    total_files = 0
    processed_files = 0

    for subdir in sorted(subdirs):
        print(f"\nProcessing directory: {subdir.name}")

        # Get all JSON files in the subdirectory
        json_files = list(subdir.glob("*.json"))

        if not json_files:
            print(f"No JSON files found in {subdir}")
            continue

        for json_file in json_files:
            total_files += 1

            # Create corresponding output path
            relative_path = json_file.relative_to(input_base)
            output_file = output_base / relative_path

            # Process the file
            process_json_file(json_file, output_file)
            processed_files += 1

    print(f"\n=== Processing Complete ===")
    print(f"Total files found: {total_files}")
    print(f"Files processed: {processed_files}")
    print(f"Output directory: {output_base}")

    return True


def main():
    """Main function to orchestrate the null value removal process."""
    print("JSON Null Value Removal Tool")
    print("=" * 40)

    # Check if dummy data should be created
    input_base = Path("twitter/json")
    has_data = False

    if input_base.exists():
        for subdir in ["00", "01", "02", "03", "04"]:
            subdir_path = input_base / subdir
            if subdir_path.exists() and list(subdir_path.glob("*.json")):
                has_data = True
                break

    if not has_data:
        print("No JSON data found")

    # Process all files
    success = process_all_files()

    if success:
        print("\n✅ All files processed successfully!")
        print("Original data preserved in: twitter/json/")
        print("Cleaned data saved to: twitter/json_without_null/")
    else:
        print("\n❌ Processing failed!")


if __name__ == "__main__":
    main()
