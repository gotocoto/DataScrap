def print_longest_lines(log_file_path):
    # Open the log file in read mode
    try:
        with open(log_file_path, 'r') as log_file:
            lines = log_file.readlines()

            # Sort lines based on their length in descending order
            lines_sorted_by_length = sorted(lines, key=len, reverse=True)

            # Print the 5 longest lines and their character counts
            print("Top 5 Longest URLS")
            for i in range(min(5, len(lines_sorted_by_length))):
                line = lines_sorted_by_length[i]
                line_length = len(line.rstrip('\n'))  # Calculate character count
                print(f"Line {i + 1}: Length={line_length} - {line.rstrip()}")

    except FileNotFoundError:
        print(f"Error: Log file '{log_file_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage:
log_file_path = 'sitemap_parser.log'
print_longest_lines(log_file_path)