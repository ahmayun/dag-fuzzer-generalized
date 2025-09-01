#!/usr/bin/env python3
"""
Script to categorize and count Apache Flink validation exceptions from log files.
"""

import sys
import re
from collections import defaultdict

def categorize(line):
    """
    Categorize validation exceptions based on the content of the line.
    
    Args:
        line (str): A line from the log file
        
    Returns:
        str: Category name for the exception type
    """
    # Skip lines that are just file paths without actual exceptions
    if not any(keyword in line for keyword in ['ValidationException:', 'Caused by:']):
        return None
    
    # Extract the actual exception message (after the file path and ::)
    if '::' in line:
        exception_part = line.split('::', 1)[1].strip()
    else:
        exception_part = line.strip()
    
    # Rule-based categorization
    if 'Unsupported argument type' in exception_part:
        return 'Unsupported argument type'
    elif 'Invalid input arguments' in exception_part:
        return 'Invalid input arguments'
    elif 'Invalid join condition' in exception_part and 'At least one equi-join predicate is required' in exception_part:
        return 'Invalid join condition'
    elif 'Invalid function call' in exception_part:
        return 'Invalid function call'
    elif 'Cannot resolve field' in exception_part:
        return 'Cannot resolve field'
    elif 'join relations with ambiguous names' in exception_part:
        return 'join relations with ambiguous names'
    elif "All types in a comparison should support 'EQUALS' comparison with each other" in exception_part:
        return "All types in a comparison should support 'EQUALS' comparison with each other"
    elif "Ambiguous column name" in exception_part:
        return "Ambiguous column name"
    else:
        # For any exception that doesn't fit our predefined categories,
        # use the entire exception message as the category name
        return exception_part

def process_log_file(file_path):
    """
    Process the log file and categorize exceptions.
    
    Args:
        file_path (str): Path to the log file
        
    Returns:
        dict: Dictionary with category counts
    """
    category_counts = defaultdict(int)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line_num, line in enumerate(file, 1):
                line = line.strip()
                if not line:
                    continue
                    
                category = categorize(line)
                if category:
                    category_counts[category] += 1
                    
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file '{file_path}': {e}")
        sys.exit(1)
    
    return category_counts

def print_results(category_counts):
    """
    Print the categorization results in a formatted way.
    
    Args:
        category_counts (dict): Dictionary with category counts
    """
    if not category_counts:
        print("No validation exceptions found in the file.")
        return
    
    print("Validation Exception Categories and Counts:")
    print("=" * 50)
    
    # Sort by count (descending) then by category name
    sorted_categories = sorted(category_counts.items(), 
                             key=lambda x: (-x[1], x[0]))
    
    for category, count in sorted_categories:
        print(f"{count}: {category}")
    
    print("=" * 50)
    print(f"Total exceptions: {sum(category_counts.values())}")

def main():
    """Main function to handle command line arguments and run the script."""
    if len(sys.argv) != 2:
        print("Usage: python script.py <log_file_path>")
        print("Example: python script.py flink_errors.txt")
        sys.exit(1)
    
    file_path = sys.argv[1]
    category_counts = process_log_file(file_path)
    print_results(category_counts)

if __name__ == "__main__":
    main()
